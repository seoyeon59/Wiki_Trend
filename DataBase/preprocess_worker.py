from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

_SCRIPT_DIR = str(Path(__file__).resolve().parent)
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

import pandas as pd
import db


def split_length(row: Any) -> pd.Series:
    if isinstance(row, dict):
        return pd.Series([row.get("old", 0), row.get("new", 0)])
    return pd.Series([0, 0])


def run_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # 기존 CSV 전처리와 동일한 삭제 규칙
    drop_cols = [
        "patrolled", "log_id", "log_type",
        "log_action", "log_params", "log_action_comment",
        "meta", "$schema"
    ]
    out = out.drop(columns=[c for c in drop_cols if c in out.columns], errors="ignore")

    # length 처리
    if "length" in out.columns:
        out[["length_old", "length_new"]] = out["length"].apply(split_length)
        out["length_old"] = out["length_old"].fillna(0).astype(int)
        out["length_new"] = out["length_new"].fillna(0).astype(int)
        out["len_diff"] = out["length_new"] - out["length_old"]
        out = out.drop(columns=["length"], errors="ignore")
    else:
        out["length_old"] = 0
        out["length_new"] = 0
        out["len_diff"] = 0

    # timestamp 처리 + 시간차 계산
    if "timestamp" in out.columns:
        out["timestamp"] = pd.to_datetime(out["timestamp"], unit="s", errors="coerce")
    out = out.sort_values(by=["title", "timestamp"], kind="mergesort")
    out["time_delta"] = out.groupby("title")["timestamp"].diff()
    out["time_delta_sec"] = out["time_delta"].dt.total_seconds().fillna(0)

    # minor 결측 처리: 기존 CSV 로직 유지
    if "minor" in out.columns:
        out["minor"] = out["minor"].fillna(False)
    else:
        out["minor"] = False

    # revision 결측 처리: 기존 CSV 로직 유지
    if "revision" in out.columns:
        out["revision"] = out["revision"].fillna(0)
    else:
        out["revision"] = 0

    # id 결측 제거
    if "id" in out.columns:
        out = out.dropna(subset=["id"])

    # 기존 CSV 코드에서 삭제하던 열 제거
    cols_to_drop = [
        "notify_url", "server_name", "server_script_path",
        "server_url", "title_url"
    ]
    out = out.drop(columns=[c for c in cols_to_drop if c in out.columns], errors="ignore")

    return out


def main() -> None:
    print("=== preprocess_worker: raw -> dataframe/csv ===", flush=True)
    conn = db.get_connection()
    try:
        raw_rows = db.fetch_raw_rows(conn)
    finally:
        conn.close()

    df = pd.DataFrame(raw_rows)
    if df.empty:
        print("No raw rows found.", flush=True)
        return

    processed = run_pipeline(df)

    print(f"Loaded raw rows: {len(df)}", flush=True)
    print(f"Processed rows: {len(processed)}", flush=True)

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", None)
    pd.set_option("display.max_colwidth", None)

    print("\n=== HEAD ===")
    print(processed.head(), flush=True)

    print("\n=== INFO ===")
    print(processed.info())

    print("\n=== NULL CHECK ===")
    print(processed.isnull().sum(), flush=True)


if __name__ == "__main__":
    main()