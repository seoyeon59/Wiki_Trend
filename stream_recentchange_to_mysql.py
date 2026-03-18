from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import pymysql
import requests


# =========================
# Configuration
# =========================

SSE_URL = os.getenv("WIKI_RC_SSE_URL", "https://stream.wikimedia.org/v2/stream/recentchange")

# MySQL connection (DB 연결 정보)
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "wikitrend")
MYSQL_TABLE = os.getenv("MYSQL_TABLE", "recentchange")

# Ingestion behavior
FILTER_WIKI = os.getenv("FILTER_WIKI", "enwiki")  # 영어 위키만 필터링
COMMIT_EVERY_N = int(os.getenv("COMMIT_EVERY_N", "50"))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "30"))
USER_AGENT = os.getenv(
    "USER_AGENT",
    "WikiTrendIngestion/1.0 (local dev; https://stream.wikimedia.org/)",
)


# =========================
# Example MySQL schema (run once)
# =========================

EXAMPLE_SCHEMA_SQL = f"""
CREATE DATABASE IF NOT EXISTS `{MYSQL_DB}` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE `{MYSQL_DB}`;

CREATE TABLE IF NOT EXISTS `{MYSQL_TABLE}` (
  `rc_id` BIGINT NOT NULL,
  `event_dt` DATETIME(6) NOT NULL,
  `wiki` VARCHAR(32) NOT NULL,
  `change_type` VARCHAR(32) NULL,
  `namespace` INT NULL,
  `title` VARCHAR(512) NULL,
  `user` VARCHAR(255) NULL,
  `bot` TINYINT(1) NOT NULL DEFAULT 0,
  `minor` TINYINT(1) NOT NULL DEFAULT 0,
  `comment` TEXT NULL,
  `server_name` VARCHAR(255) NULL,
  `server_url` VARCHAR(255) NULL,
  `revision_old` BIGINT NULL,
  `revision_new` BIGINT NULL,
  `raw_json` JSON NULL,
  PRIMARY KEY (`rc_id`),
  KEY `idx_event_dt` (`event_dt`),
  KEY `idx_wiki_dt` (`wiki`, `event_dt`),
  KEY `idx_title` (`title`(191))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""".strip()


# =========================
# Helpers
# =========================


def connect_mysql() -> pymysql.connections.Connection:
    """
    MySQL에 정식 연결. utf8mb4 인코딩으로 이모지 포함 모든 유니코드 지원.
    """
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )


def connect_mysql_no_db() -> pymysql.connections.Connection:
    """
    DB를 지정하지 않고 MySQL 서버 자체에만 연결.
    테이블/DB가 아직 없는 최초 실행 시 스키마 생성 전용으로 사용.
    """
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


def ensure_schema_exists() -> None:
    """
    B와 테이블이 없으면 자동으로 생성해주는 초기화 함수.
    IF NOT EXISTS 구문이라 이미 있어도 오류 없이 안전하게 실행됨.
    """
    conn = connect_mysql_no_db()
    try:
        with conn.cursor() as cur:
            for stmt in EXAMPLE_SCHEMA_SQL.split(";"):
                sql = stmt.strip()
                if not sql:
                    continue
                cur.execute(sql)
    finally:
        conn.close()


def parse_event_datetime(ts: Any) -> datetime:
    """
    위키미디어가 보내는 Unix 타임스탬프(정수 초)를
    MySQL에 저장 가능한 UTC datetime 객체로 변환
    """
    try:
        ts_int = int(ts)
    except Exception:
        # Fallback to "now" if timestamp missing/invalid
        return datetime.now(timezone.utc)
    return datetime.fromtimestamp(ts_int, tz=timezone.utc)


def extract_fields(event: Dict[str, Any]) -> Optional[Tuple]:
    """
    SSE로 받은 JSON 이벤트에서 DB에 저장할 컬럼만 뽑아내는 함수.
    FILTER_WIKI 조건 (:enwiki)에 맞지 않으면 None 반환해서 스킵
    """
    if FILTER_WIKI and event.get("wiki") != FILTER_WIKI:
        return None

    # Unique id for dedupe
    rc_id = event.get("id")
    if rc_id is None:
        return None

    meta = event.get("meta") or {}
    revision = event.get("revision") or {}

    event_dt = parse_event_datetime(event.get("timestamp"))

    return (
        int(rc_id),
        event_dt.strftime("%Y-%m-%d %H:%M:%S.%f"),
        (event.get("wiki") or "")[:32],
        event.get("type"),
        event.get("namespace"),
        event.get("title"),
        event.get("user"),
        1 if event.get("bot") else 0,
        1 if event.get("minor") else 0,
        event.get("comment"),
        meta.get("domain") or meta.get("server_name") or meta.get("host"),
        meta.get("uri") or meta.get("server_url"),
        revision.get("old"),
        revision.get("new"),
        json.dumps(event, ensure_ascii=False),
    )


INSERT_SQL = f"""
INSERT IGNORE INTO `{MYSQL_TABLE}`
(
  rc_id, event_dt, wiki, change_type, namespace, title, user, bot, minor, comment,
  server_name, server_url, revision_old, revision_new, raw_json
)
VALUES
(
  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
  %s, %s, %s, %s, CAST(%s AS JSON)
)
""".strip()


def stream_lines() -> Any:
    """
    위키미디어 SSE URL에 HTTP 스트림 연결을 열고, 한 줄씩 읽어서 위로 올려주는(yield) 함수.
    빈 줄, None 줄은 무시하고 의미 있는 줄만 반환.
    """
    headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream"}
    with requests.get(
        SSE_URL,
        headers=headers,
        stream=True,
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


def main() -> None:

    # ① 시작 시 설정값 출력 (URL, DB 정보, 필터 조건 등 확인용)
    print("=== Wiki RecentChange -> MySQL ingester ===", flush=True)
    print(f"SSE_URL={SSE_URL}", flush=True)
    print(
        f"MYSQL={MYSQL_USER}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB} table={MYSQL_TABLE}",
        flush=True,
    )
    print(f"FILTER_WIKI={FILTER_WIKI!r} (set FILTER_WIKI='' to disable)", flush=True)
    print("", flush=True)
    print("Run this once to create schema (example):", flush=True)
    print(EXAMPLE_SCHEMA_SQL, flush=True)
    print("", flush=True)

    conn = None   # MySQL 연결 객체 (처음엔 없음)
    backoff = 1.0  # 재연결 대기 시간 초기값 (1초)
    max_backoff = 30.0  # 재연결 대시 시간 최대값 (30초)
    inserted_since_commit = 0  # 마지막 commit 이후 삽입 건수
    total_inserted = 0  # 전체 누적 삽입 건수
    total_seen = 0  # 전체 수신 이벤트 수 (필터 전)

    while True:   # ② 프로그램이 꺼질 때까지 무한 반복
        try:
            if conn is None or not conn.open:  # ③ MySQL 연결이 없거나 끊겼으면 재연결 시도
                try:
                    conn = connect_mysql()
                except pymysql.err.OperationalError as e:
                    # ④ DB가 아예 없는 경우(에러코드 1049) → 스키마 자동 생성 후 재연결
                    if len(e.args) >= 1 and e.args[0] == 1049:
                        print(
                            f"Database {MYSQL_DB!r} not found; creating schema...",
                            flush=True,
                        )
                        ensure_schema_exists()
                        conn = connect_mysql()
                    else:
                        raise  # 다른 DB 오류는 그냥 위로 던짐
                backoff = 1.0  # 연결 성공 시 backoff 초기화
                print("Connected to MySQL.", flush=True)

            with conn.cursor() as cur:
                print("Connecting to SSE stream...", flush=True)  # ⑤ SSE 스트림에서 한 줄씩 읽기 시작
                for line in stream_lines():
                    if not line.startswith("data:"):  # ⑥ "data:"로 시작하는 줄만 처리 (SSE 포맷 규칙)
                        continue

                    payload = line[5:].lstrip()  # "data: {...}" 에서 앞 5글자 제거
                    if payload == "[DONE]":  # 스트림 종료 신호 무시
                        continue

                    # ⑦ JSON 파싱 실패하면 해당 줄만 스킵 (프로그램은 유지)
                    try:
                        event = json.loads(payload)
                    except json.JSONDecodeError:
                        continue

                    total_seen += 1
                    row = extract_fields(event)   # ⑧ 필드 추출 (필터 조건 불일치 or rc_id 없으면 None → 스킵)
                    if row is None:
                        continue

                    cur.execute(INSERT_SQL, row)   # ⑨ DB에 삽입 (INSERT IGNORE → 중복 rc_id는 조용히 무시)
                    if cur.rowcount == 1:  # 실제로 삽입된 경우만 카운트
                        inserted_since_commit += 1
                        total_inserted += 1

                    # ⑩ N건마다 일괄 commit (매번 commit하면 I/O 부담 큼)
                    if inserted_since_commit >= COMMIT_EVERY_N:
                        conn.commit()
                        inserted_since_commit = 0
                        print(
                            f"Committed batch. seen={total_seen} inserted={total_inserted} "
                            f"last_title={event.get('title', 'N/A')!r}",
                            flush=True,
                        )

        except KeyboardInterrupt:
            print("\nStopping (Ctrl+C).", flush=True)  # ⑪ Ctrl+C 입력 시 → 남은 데이터 commit 후 정상 종료
            try:
                if conn is not None and conn.open:
                    if inserted_since_commit > 0:
                        conn.commit()
                    conn.close()
            finally:
                return
        except (requests.RequestException, pymysql.MySQLError) as e:
            print(f"Error: {e.__class__.__name__}: {e}", flush=True)
            # ⑫ 네트워크 or DB 오류 → 남은 데이터 commit, 연결 닫고 backoff 후 재시도
            # backoff는 1초 → 2초 → 4초 → ... → 최대 30초로 지수 증가
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
            # ⑬ 예상 못한 오류(파싱 버그 등) → 로그만 남기고 1초 후 계속 실행 (프로그램이 죽지 않고 유지되는 게 목적)
            print(f"Unexpected error: {e.__class__.__name__}: {e}", flush=True)
            time.sleep(1.0)


if __name__ == "__main__":
    main()

