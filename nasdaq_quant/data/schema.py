"""
data/schema.py
SQLite 스키마 초기화 — bars_1min, bars_daily 테이블
"""
import sqlite3
from pathlib import Path


DDL_BARS_1MIN = """
CREATE TABLE IF NOT EXISTS bars_1min (
    ticker    TEXT    NOT NULL,
    date      TEXT    NOT NULL,   -- YYYY-MM-DD (ET 기준)
    timestamp TEXT    NOT NULL,   -- ISO8601 with tz, ET 기준
    open      REAL    NOT NULL,
    high      REAL    NOT NULL,
    low       REAL    NOT NULL,
    close     REAL    NOT NULL,
    volume    INTEGER NOT NULL,
    PRIMARY KEY (ticker, timestamp)
);
"""

DDL_BARS_DAILY = """
CREATE TABLE IF NOT EXISTS bars_daily (
    ticker TEXT NOT NULL,
    date   TEXT NOT NULL,   -- YYYY-MM-DD
    open   REAL NOT NULL,
    high   REAL NOT NULL,
    low    REAL NOT NULL,
    close  REAL NOT NULL,
    volume INTEGER NOT NULL,
    PRIMARY KEY (ticker, date)
);
"""

DDL_INDEX_1MIN_DATE = """
CREATE INDEX IF NOT EXISTS idx_bars_1min_ticker_date
ON bars_1min (ticker, date);
"""

DDL_INDEX_DAILY = """
CREATE INDEX IF NOT EXISTS idx_bars_daily_date
ON bars_daily (date);
"""


def init_db(db_path: Path) -> None:
    """DB 파일이 없으면 생성, 테이블·인덱스 초기화"""
    with sqlite3.connect(db_path) as conn:
        conn.execute(DDL_BARS_1MIN)
        conn.execute(DDL_BARS_DAILY)
        conn.execute(DDL_INDEX_1MIN_DATE)
        conn.execute(DDL_INDEX_DAILY)
        conn.commit()


def validate_1min_row(row: dict) -> list[str]:
    """
    단일 1분봉 행의 스키마 유효성 검사.
    반환: 오류 메시지 리스트 (빈 리스트 = 정상)
    """
    errors = []
    required = {"ticker", "date", "timestamp", "open", "high", "low", "close", "volume"}
    missing = required - set(row.keys())
    if missing:
        errors.append(f"누락 컬럼: {missing}")
        return errors  # 이후 검사 불가

    if not isinstance(row["ticker"], str) or not row["ticker"]:
        errors.append("ticker는 비어 있지 않은 문자열이어야 합니다")
    if row["high"] < row["low"]:
        errors.append(f"high({row['high']}) < low({row['low']})")
    if row["open"] <= 0 or row["close"] <= 0:
        errors.append("open/close는 양수여야 합니다")
    if row["volume"] < 0:
        errors.append("volume은 0 이상이어야 합니다")
    return errors
