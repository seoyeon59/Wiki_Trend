from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Set, Tuple

# 이 파일이 절대 경로나 동적 로더로 실행될 때도 같은 디렉터리의 db.py를 찾을 수 있도록
# 스크립트 디렉터리를 sys.path 맨 앞에 추가한다.
_SCRIPT_DIR = str(Path(__file__).resolve().parent)
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

import pandas as pd

import db

# ---------------------------------------------------------------------------
# 전처리 파이프라인 설정
# ---------------------------------------------------------------------------

# processed 테이블에 저장할 컬럼 목록 — 이 외의 컬럼은 노이즈/메타 유출로 간주해 제거한다.
_PROCESSED_BASE_COLUMNS: Tuple[str, ...] = (
    "id",
    "type",
    "namespace",
    "title",
    "title_url",
    "comment",
    "timestamp",
    "user",
    "bot",
    "notify_url",
    "length",
    "revision",
    "server_url",
    "server_name",
    "server_script_path",
    "wiki",
    "parsedcomment",
)

# SSE/Kafka 봉투(envelope) 필드 — DataFrame에 포함되어 있으면 제거 대상
_META_LIKE_COLUMNS: Set[str] = {
    "meta",
    "$schema",
    "schema",
    "dt",
    "partition",
    "offset",
    "request_id",
}


def _parse_json_dict(val: Any) -> Optional[Dict[str, Any]]:
    """MySQL JSON 컬럼값(str/dict/None)을 딕셔너리로 정규화한다. 파싱 불가능하거나 None이면 None을 반환한다."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None
    return None


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    불필요한 컬럼을 제거하고 id 컬럼을 정수형으로 정규화한다.

    - 메타/봉투 컬럼(_META_LIKE_COLUMNS) 제거
    - _PROCESSED_BASE_COLUMNS 외의 예상치 못한 컬럼 제거
    - id가 없거나 숫자로 변환 불가한 행 제거
    """
    out = df.copy()

    # 메타/봉투 컬럼 제거
    drop_cols = [c for c in out.columns if c in _META_LIKE_COLUMNS]
    if drop_cols:
        out = out.drop(columns=drop_cols)

    # 알려진 컬럼만 남기고 예상치 못한 컬럼은 제거
    keep = [c for c in _PROCESSED_BASE_COLUMNS if c in out.columns]
    extra = [c for c in out.columns if c not in _PROCESSED_BASE_COLUMNS and c not in keep]
    if extra:
        out = out.drop(columns=extra)

    if "id" not in out.columns:
        raise ValueError("DataFrame must contain an 'id' column")

    out = out.dropna(subset=["id"])
    # id가 없거나 숫자 변환 불가한 행 제거 후 DB PK용 int64로 변환
    out["id"] = pd.to_numeric(out["id"], errors="coerce")
    out = out.dropna(subset=["id"])
    out["id"] = out["id"].astype("int64")
    return out


def process_length(df: pd.DataFrame) -> pd.DataFrame:
    """
    JSON 형태의 length/revision 컬럼을 분석 가능한 개별 컬럼으로 펼친다.

    - length  → length_old, length_new, len_diff (= new - old)
    - revision → revision_old, revision_new
    원본 JSON 컬럼은 변환 후 삭제한다.
    """
    out = df.copy()

    def split_old_new(series: pd.Series) -> pd.DataFrame:
        olds: list[Any] = []
        news: list[Any] = []
        for v in series:
            d = _parse_json_dict(v)
            if d is None:
                olds.append(None)
                news.append(None)
            else:
                olds.append(d.get("old"))
                news.append(d.get("new"))
        return pd.DataFrame({"old": olds, "new": news})

    if "length" in out.columns:
        lo = split_old_new(out["length"])
        out["length_old"] = pd.to_numeric(lo["old"], errors="coerce")
        out["length_new"] = pd.to_numeric(lo["new"], errors="coerce")
        out["len_diff"] = out["length_new"] - out["length_old"] # 편집으로 인한 길이 변화량
        out = out.drop(columns=["length"])
    else:
        out["length_old"] = pd.NA
        out["length_new"] = pd.NA
        out["len_diff"] = pd.NA

    if "revision" in out.columns:
        ro = split_old_new(out["revision"])
        out["revision_old"] = pd.to_numeric(ro["old"], errors="coerce")
        out["revision_new"] = pd.to_numeric(ro["new"], errors="coerce")
        out = out.drop(columns=["revision"])
    else:
        out["revision_old"] = pd.NA
        out["revision_new"] = pd.NA

    return out


def compute_time_delta(df: pd.DataFrame) -> pd.DataFrame:
    """
    같은 문서(title) 내에서 직전 편집과의 시간 간격(초)을 계산해 time_delta_sec 컬럼에 저장한다.
    문서별 첫 번째 편집은 NaN → 0으로 채운다.
    """
    out = df.copy()
    if "timestamp" not in out.columns or "title" not in out.columns:
        out["time_delta_sec"] = 0.0
        return out

    out = out.sort_values(["timestamp", "id"], kind="mergesort")
    ts = pd.to_numeric(out["timestamp"], errors="coerce")
    delta = ts.groupby(out["title"]).diff()
    out["time_delta_sec"] = delta
    out["time_delta_sec"] = out["time_delta_sec"].fillna(0)
    return out


def run_pipeline(rows: Iterable[Dict[str, Any]]) -> pd.DataFrame:
    """
    raw 행 목록을 받아 전처리 파이프라인 전체를 실행한다.
    순서: 컬럼 정리 → length/revision 전개 → 시간 간격 계산
    """
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = clean_columns(df)
    df = process_length(df)
    df = compute_time_delta(df)
    return df


def _rows_for_db(df: pd.DataFrame) -> Iterable[Dict[str, Any]]:
    """
    DataFrame 행을 PyMySQL이 처리할 수 있는 Python 기본 타입 딕셔너리로 변환한다.
    - NaN/NA → None (SQL NULL)
    - numpy 스칼라 → Python 기본형 (.item() 호출)
    """
    cols = [
        "id",
        "type",
        "namespace",
        "title",
        "title_url",
        "comment",
        "timestamp",
        "user",
        "bot",
        "notify_url",
        "length_old",
        "length_new",
        "len_diff",
        "revision_old",
        "revision_new",
        "server_url",
        "server_name",
        "server_script_path",
        "wiki",
        "parsedcomment",
        "time_delta_sec",
    ]
    sub = df.reindex(columns=cols)
    for _, row in sub.iterrows():
        d = row.to_dict()
        for k, v in list(d.items()):
            if pd.isna(v):
                d[k] = None
            elif hasattr(v, "item"):
                try:
                    d[k] = v.item()
                except (ValueError, AttributeError):
                    pass
        yield d


def main() -> None:
    """raw 테이블 데이터를 전처리해 processed 테이블에 upsert한다."""
    print("=== preprocess_worker: raw -> processed ===", flush=True)
    db.ensure_schema_exists()
    conn = db.get_connection()
    try:
        raw_rows = db.fetch_raw_rows(conn)
        print(f"Loaded {len(raw_rows)} raw row(s).", flush=True)
        processed = run_pipeline(raw_rows)
        if processed.empty:
            print("Nothing to write.", flush=True)
            return
        n = db.insert_processed_rows(conn, _rows_for_db(processed))
        print(f"Upserted {n} processed row(s) into `{db.MYSQL_PROCESSED_TABLE}`.", flush=True)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
