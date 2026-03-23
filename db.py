from __future__ import annotations

import os
from typing import Any, Dict, Iterable, List, Tuple

import pymysql


# ---------------------------------------------------------------------------
# DB 접속 설정 (환경변수 우선, 없으면 로컬 개발용 기본값 사용)
# ---------------------------------------------------------------------------
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "wikitrend")
# Raw wiki events (no preprocessing at insert time)
MYSQL_RAW_TABLE = os.getenv("MYSQL_RAW_TABLE", "recentchange_raw")
# Preprocessed output
MYSQL_PROCESSED_TABLE = os.getenv("MYSQL_PROCESSED_TABLE", "processed")


def get_connection() -> pymysql.connections.Connection:
    """지정된 DB에 utf8mb4로 접속한다. autocommit=False로 트랜잭션을 수동 관리한다."""
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


def get_connection_no_db() -> pymysql.connections.Connection:
    """DB를 지정하지 않고 접속한다. 최초 스키마 생성(CREATE DATABASE) 시 사용한다."""
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


def _schema_ddl() -> str:
    """DB와 두 테이블(raw, processed)을 생성하는 DDL을 반환한다. IF NOT EXISTS 조건을 사용하므로 이미 존재해도 안전하게 실행된다."""
    return f"""
CREATE DATABASE IF NOT EXISTS `{MYSQL_DB}` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE `{MYSQL_DB}`;

CREATE TABLE IF NOT EXISTS `{MYSQL_RAW_TABLE}` (
  `id` BIGINT NOT NULL,
  `type` VARCHAR(64) NULL,
  `namespace` INT NULL,
  `title` VARCHAR(512) NULL,
  `title_url` TEXT NULL,
  `comment` TEXT NULL,
  `timestamp` BIGINT NULL,
  `user` VARCHAR(255) NULL,
  `bot` TINYINT(1) NOT NULL DEFAULT 0,
  `notify_url` TEXT NULL,
  `length` JSON NULL,
  `revision` JSON NULL,
  `server_url` TEXT NULL,
  `server_name` VARCHAR(255) NULL,
  `server_script_path` VARCHAR(512) NULL,
  `wiki` VARCHAR(32) NULL,
  `parsedcomment` TEXT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_ts` (`timestamp`),
  KEY `idx_wiki_ts` (`wiki`, `timestamp`),
  KEY `idx_title` (`title`(191))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `{MYSQL_PROCESSED_TABLE}` (
  `id` BIGINT NOT NULL,
  `type` VARCHAR(64) NULL,
  `namespace` INT NULL,
  `title` VARCHAR(512) NULL,
  `title_url` TEXT NULL,
  `comment` TEXT NULL,
  `timestamp` BIGINT NULL,
  `user` VARCHAR(255) NULL,
  `bot` TINYINT(1) NOT NULL DEFAULT 0,
  `notify_url` TEXT NULL,
  `length_old` BIGINT NULL,
  `length_new` BIGINT NULL,
  `len_diff` BIGINT NULL,
  `revision_old` BIGINT NULL,
  `revision_new` BIGINT NULL,
  `server_url` TEXT NULL,
  `server_name` VARCHAR(255) NULL,
  `server_script_path` VARCHAR(512) NULL,
  `wiki` VARCHAR(32) NULL,
  `parsedcomment` TEXT NULL,
  `time_delta_sec` DOUBLE NULL,
  PRIMARY KEY (`id`),
  KEY `idx_ts` (`timestamp`),
  KEY `idx_title` (`title`(191))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""".strip()


def ensure_schema_exists() -> None:
    """DB와 테이블이 없으면 생성한다. DDL을 ';' 단위로 분리해 순서대로 실행한다."""
    conn = get_connection_no_db()
    try:
        with conn.cursor() as cur:
            for stmt in _schema_ddl().split(";"):
                sql = stmt.strip()
                if not sql:
                    continue
                cur.execute(sql)
    finally:
        conn.close()


# raw_ingest.py의 extract_raw_row() 반환 튜플과 컬럼 순서가 반드시 일치해야 한다.
RAW_INSERT_SQL = f"""
INSERT IGNORE INTO `{MYSQL_RAW_TABLE}` (
  id, type, namespace, title, title_url, comment, timestamp,
  user, bot, notify_url, length, revision,
  server_url, server_name, server_script_path, wiki, parsedcomment
) VALUES (
  %s, %s, %s, %s, %s, %s, %s,
  %s, %s, %s, CAST(%s AS JSON), CAST(%s AS JSON),
  %s, %s, %s, %s, %s
)
""".strip()


def insert_raw_row(cursor: pymysql.cursors.Cursor, row: Tuple[Any, ...]) -> None:
    """원시 이벤트 1건을 삽입한다. 커밋은 호출자(배치 단위)가 담당한다."""
    cursor.execute(RAW_INSERT_SQL, row)


def fetch_raw_rows(conn: pymysql.connections.Connection) -> List[Dict[str, Any]]:
    """전처리 워커가 사용할 raw 테이블 전체 데이터를 읽어온다."""
    sql = f"SELECT * FROM `{MYSQL_RAW_TABLE}`"
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return list(rows)


# 전처리 테이블 컬럼 순서 — _processed_placeholder_row()에서 딕셔너리를 튜플로 변환할 때 사용
_PROCESSED_COLS: Tuple[str, ...] = (
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
)


def _processed_placeholder_row(
    row: Dict[str, Any],
) -> Tuple[Any, ...]:
    return tuple(row[c] for c in _PROCESSED_COLS)


def insert_processed_rows(
    conn: pymysql.connections.Connection,
    rows: Iterable[Dict[str, Any]],
    *,
    chunk_size: int = 500,
) -> int:
    """
    전처리된 행을 processed 테이블에 upsert한다.
    ON DUPLICATE KEY UPDATE를 사용하므로 워커를 재실행해도 중복 없이 안전하게 갱신된다.
    반환값: DB에 전송한 총 행 수 (실제 변경된 행 수와 다를 수 있음)
    """
    cols_sql = ", ".join(f"`{c}`" for c in _PROCESSED_COLS)
    placeholders = ", ".join(["%s"] * len(_PROCESSED_COLS))
    updates = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in _PROCESSED_COLS if c != "id")
    sql = (
        f"INSERT INTO `{MYSQL_PROCESSED_TABLE}` ({cols_sql}) VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {updates}"
    )

    batch: List[Tuple[Any, ...]] = []
    total = 0
    with conn.cursor() as cur:
        for r in rows:
            batch.append(_processed_placeholder_row(r))
            if len(batch) >= chunk_size:
                cur.executemany(sql, batch)
                total += len(batch)
                batch.clear()
        if batch:
            cur.executemany(sql, batch)
            total += len(batch)
    conn.commit()
    return total


def main() -> None:
    """단독 실행 시 스키마 존재 여부를 확인하고 없으면 생성한다."""
    print("=== db.py: ensuring schema ===", flush=True)
    print(
        f"MYSQL={MYSQL_USER}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB} "
        f"raw={MYSQL_RAW_TABLE} processed={MYSQL_PROCESSED_TABLE}",
        flush=True,
    )
    ensure_schema_exists()
    print("Schema OK.", flush=True)


if __name__ == "__main__":
    main()
