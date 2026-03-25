from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

# 이 파일이 절대 경로나 동적 로더로 실행될 때도 같은 디렉터리의 db.py를 찾을 수 있도록
# 스크립트 디렉터리를 sys.path 맨 앞에 추가한다.
_SCRIPT_DIR = str(Path(__file__).resolve().parent)
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

import pymysql
import requests

import db

# ---------------------------------------------------------------------------
# 설정값
# ---------------------------------------------------------------------------

# 위키미디어 실시간 변경 이벤트를 수신할 SSE 엔드포인트 URL
SSE_URL = os.getenv("WIKI_RC_SSE_URL", "https://stream.wikimedia.org/v2/stream/recentchange")

FILTER_WIKI = os.getenv("FILTER_WIKI", "enwiki")  # 수집 대상 위키 (기본값: 영문 위키피디아)
COMMIT_EVERY_N = int(os.getenv("COMMIT_EVERY_N", "50")) # N건 삽입마다 한 번씩 DB에 커밋
REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "30")) # SSE HTTP 요청 타임아웃 (초)
USER_AGENT = os.getenv(
    "USER_AGENT",
    "WikiTrendIngestion/1.0 (local dev; https://stream.wikimedia.org/)",
)  # 위키미디어 API 정책에 따른 User-Agent 헤더


def stream_lines():
    """
    위키미디어 SSE 스트림에 접속하여 비어 있지 않은 텍스트 라인을 순서대로 yield한다.
    스트림은 무한히 이어지므로 호출자가 루프를 끊을 책임을 진다.
    """
    headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream"}
    with requests.get(
        SSE_URL,
        headers=headers,
        stream=True,       # 응답 본문을 즉시 다운로드하지 않고 스트리밍으로 수신
        timeout=REQUEST_TIMEOUT_SECONDS,
    ) as resp:
        resp.raise_for_status()
        for raw_line in resp.iter_lines(decode_unicode=True):
            if raw_line is None:
                continue
            line = raw_line.strip()
            if not line:
                continue
            yield line


def _json_or_none(value: Any) -> Optional[str]:
    """
    dict/list 타입 값을 MySQL JSON 컬럼에 저장하기 위해 JSON 문자열로 직렬화한다.
    None은 그대로 None으로 반환하고, 예상치 못한 스칼라 값은 추적을 위해 JSON 문자열로 변환한다.
    """
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return json.dumps(value, ensure_ascii=False)


def extract_raw_row(event: Dict[str, Any]) -> Optional[Tuple[Any, ...]]:
    """
    RecentChange 이벤트 JSON 한 건을 DB 컬럼 순서에 맞는 튜플로 변환한다.

    - 지정된 위키(FILTER_WIKI)가 아니거나 이벤트 ID가 없는 경우 None을 반환해 삽입을 건너뛴다.
    - 중첩 객체(length, revision)는 전처리 없이 JSON 문자열 그대로 저장한다.
    - timestamp는 datetime 변환 없이 Unix 초 정수로 그대로 보존한다.
    """
    if FILTER_WIKI and event.get("wiki") != FILTER_WIKI:     # 대상 위키 필터링: 지정된 위키가 아니면 무시
        return None

    rc_id = event.get("id")   # 이벤트 고유 ID가 없으면 저장 불가 → 건너뜀
    if rc_id is None:
        return None

    meta = event.get("meta") or {}    # 이벤트 메타데이터 서브객체 (없으면 빈 dict로 대체)

    # timestamp: datetime 변환 없이 Unix 초 정수로 저장
    ts = event.get("timestamp")
    try:
        ts_val = int(ts) if ts is not None else None
    except (TypeError, ValueError):
        ts_val = None

    # 여러 필드에서 URL 정보를 우선순위에 따라 추출 (이벤트 본문 → meta 서브객체 순)
    title_url = event.get("title_url") or meta.get("uri")
    notify_url = event.get("notify_url") or meta.get("notify_url")
    server_url = event.get("server_url") or meta.get("server_url") or meta.get("uri")
    server_name = (
        event.get("server_name")
        or meta.get("domain")
        or meta.get("server_name")
        or meta.get("host")
    )
    server_script_path = event.get("server_script_path")

    minor_raw = event.get("minor")
    minor = 1 if minor_raw is True else 0 if minor_raw is False else None

    # DB 컬럼 순서에 맞춰 튜플로 반환
    return (
        int(rc_id),
        event.get("type"),
        event.get("namespace"),
        event.get("title"),
        title_url,
        event.get("comment"),
        ts_val,
        event.get("user"),
        1 if event.get("bot") else 0,
        minor,
        notify_url,
        _json_or_none(event.get("length")),
        _json_or_none(event.get("revision")),
        server_url,
        server_name,
        server_script_path,
        (event.get("wiki") or "")[:32] or None,
        event.get("parsedcomment"),
    )


def main() -> None:
    # 시작 시 주요 설정값을 출력해 운영 중 문제 파악을 쉽게 한다.
    print("=== Wiki RecentChange -> MySQL raw ingest ===", flush=True)
    print(f"SSE_URL={SSE_URL}", flush=True)
    print(
        f"MYSQL={db.MYSQL_USER}@{db.MYSQL_HOST}:{db.MYSQL_PORT}/{db.MYSQL_DB} "
        f"table={db.MYSQL_RAW_TABLE}",
        flush=True,
    )
    print(f"FILTER_WIKI={FILTER_WIKI!r} (set FILTER_WIKI='' to disable)", flush=True)
    print("", flush=True)
    print("Schema DDL: run `python db.py` or rely on auto-create on first connect.", flush=True)
    print("", flush=True)

    conn = None
    backoff = 1.0       # 재접속 대기 시간 초기값 (초)
    max_backoff = 30.0  # 재접속 대기 시간 최대값 (초)
    inserted_since_commit = 0   # 마지막 커밋 이후 삽입된 행 수
    total_inserted = 0          # 프로세스 시작 이후 총 삽입 수
    total_seen = 0              # 수신한 전체 이벤트 수 (필터 제외 전)

    while True:
        try:
            if conn is None or not conn.open:       # DB 연결이 없거나 끊긴 경우 재연결 시도
                try:
                    conn = db.get_connection()
                except pymysql.err.OperationalError as e:
                    if len(e.args) >= 1 and e.args[0] == 1049:      # 데이터베이스 자체가 없는 경우(1049) 스키마를 자동 생성 후 재시도
                        print(
                            f"Database {db.MYSQL_DB!r} not found; creating schema...",
                            flush=True,
                        )
                        db.ensure_schema_exists()
                        conn = db.get_connection()
                    else:
                        raise
                backoff = 1.0       # 연결 성공 시 backoff 초기화
                print("Connected to MySQL.", flush=True)

            with conn.cursor() as cur:
                print("Connecting to SSE stream...", flush=True)
                for line in stream_lines():
                    if not line.startswith("data:"):
                        continue

                    payload = line[5:].lstrip()
                    if payload == "[DONE]":
                        continue

                    try:
                        event = json.loads(payload)
                    except json.JSONDecodeError:
                        continue

                    total_seen += 1
                    row = extract_raw_row(event)
                    if row is None:
                        continue

                    db.insert_raw_row(cur, row)
                    if cur.rowcount == 1:
                        inserted_since_commit += 1
                        total_inserted += 1

                    if inserted_since_commit >= COMMIT_EVERY_N:
                        conn.commit()
                        inserted_since_commit = 0
                        print(
                            f"Committed batch. seen={total_seen} inserted={total_inserted} "
                            f"last_title={event.get('title', 'N/A')!r}",
                            flush=True,
                        )

        except KeyboardInterrupt:
            print("\nStopping (Ctrl+C).", flush=True)      # Ctrl+C 로 정상 종료 시 미커밋 데이터를 마지막으로 한 번 커밋하고 종료
            try:
                if conn is not None and conn.open:
                    if inserted_since_commit > 0:
                        conn.commit()
                    conn.close()
            finally:
                return
        except (requests.RequestException, pymysql.MySQLError) as e:
            print(f"Error: {e.__class__.__name__}: {e}", flush=True)    # 네트워크 오류 또는 DB 오류 발생 시: 미커밋 데이터 저장 후 지수 백오프로 재접속
            try:
                if conn is not None and conn.open:
                    if inserted_since_commit > 0:
                        conn.commit()
                        inserted_since_commit = 0
                    conn.close()
            except Exception:
                pass
            conn = None
            print(f"Reconnecting after {backoff:.1f}s...", flush=True)
            time.sleep(backoff)
            backoff = min(max_backoff, backoff * 2)
        except Exception as e:
            print(f"Unexpected error: {e.__class__.__name__}: {e}", flush=True) # 예상치 못한 예외는 로그만 남기고 1초 후 루프 재시작
            time.sleep(1.0)


if __name__ == "__main__":
    main()
