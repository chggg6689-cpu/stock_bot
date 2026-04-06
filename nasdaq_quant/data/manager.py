"""
data/manager.py
DataManager — 1분봉·일봉 캐시 관리 (SQLite + yfinance)
"""
import sqlite3
import warnings
import logging
from datetime import date, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf

from nasdaq_quant.data.schema import init_db, validate_1min_row

warnings.filterwarnings("ignore")
log = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")
DEFAULT_DB = Path(__file__).parent.parent.parent / "quant_data.db"


class DataManager:
    def __init__(self, db_path: Path = DEFAULT_DB):
        self.db_path = db_path
        init_db(db_path)

    # ─────────────────────────────────────────
    # 1분봉
    # ─────────────────────────────────────────
    def get_1min(self, ticker: str, for_date: date) -> pd.DataFrame:
        """
        ET 기준 for_date 당일 1분봉 반환.
        캐시(SQLite) 우선 → 미스 시 yfinance 조회 후 캐시 저장.
        반환 DataFrame index: DatetimeTZAware(ET), columns: Open/High/Low/Close/Volume
        """
        df = self._load_1min_cache(ticker, for_date)
        if df is not None and not df.empty:
            log.debug("%s %s: 캐시 히트 (%d행)", ticker, for_date, len(df))
            return df

        log.debug("%s %s: 캐시 미스 → yfinance 조회", ticker, for_date)
        df = self._fetch_1min_yfinance(ticker, for_date)
        if df is not None and not df.empty:
            self.cache_1min(ticker, for_date, df)
        return df if df is not None else pd.DataFrame()

    def cache_1min(self, ticker: str, for_date: date, df: pd.DataFrame) -> None:
        """
        1분봉 DataFrame을 SQLite에 저장.
        df index: DatetimeTZAware, columns: Open/High/Low/Close/Volume
        """
        rows = []
        date_str = for_date.isoformat()
        for ts, row in df.iterrows():
            r = {
                "ticker":    ticker,
                "date":      date_str,
                "timestamp": ts.isoformat(),
                "open":      float(row.get("Open",   row.get("open",   0))),
                "high":      float(row.get("High",   row.get("high",   0))),
                "low":       float(row.get("Low",    row.get("low",    0))),
                "close":     float(row.get("Close",  row.get("close",  0))),
                "volume":    int(row.get("Volume", row.get("volume", 0))),
            }
            errs = validate_1min_row(r)
            if errs:
                log.warning("%s %s 행 건너뜀: %s", ticker, ts, errs)
                continue
            rows.append(r)

        if not rows:
            return

        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                """INSERT OR IGNORE INTO bars_1min
                   (ticker, date, timestamp, open, high, low, close, volume)
                   VALUES (:ticker, :date, :timestamp, :open, :high, :low, :close, :volume)""",
                rows,
            )
            conn.commit()
        log.debug("%s %s: %d행 캐시 저장", ticker, for_date, len(rows))

    def _load_1min_cache(self, ticker: str, for_date: date) -> pd.DataFrame | None:
        """SQLite에서 1분봉 조회. 데이터 없으면 None 반환."""
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query(
                "SELECT timestamp, open, high, low, close, volume "
                "FROM bars_1min WHERE ticker=? AND date=? ORDER BY timestamp",
                conn,
                params=(ticker, for_date.isoformat()),
            )
        if df.empty:
            return None
        df.index = pd.to_datetime(df["timestamp"]).dt.tz_convert(ET)
        df = df.drop(columns=["timestamp"])
        df.columns = ["Open", "High", "Low", "Close", "Volume"]
        return df

    def _fetch_1min_yfinance(self, ticker: str, for_date: date) -> pd.DataFrame | None:
        """yfinance에서 1분봉 조회. 실패 시 None 반환."""
        try:
            raw = yf.download(
                ticker,
                start=for_date.strftime("%Y-%m-%d"),
                end=(for_date + timedelta(days=1)).strftime("%Y-%m-%d"),
                interval="1m",
                auto_adjust=True,
                progress=False,
            )
            if raw.empty:
                return None
            if isinstance(raw.columns, pd.MultiIndex):
                raw.columns = raw.columns.get_level_values(0)
            raw.index = pd.to_datetime(raw.index).tz_convert(ET)
            return raw
        except Exception as e:
            log.warning("%s 1분봉 조회 실패: %s", ticker, e)
            return None

    # ─────────────────────────────────────────
    # 일봉
    # ─────────────────────────────────────────
    def get_daily(
        self,
        tickers: list[str],
        start: date,
        end: date,
    ) -> pd.DataFrame:
        """
        일봉 배치 조회 (캐시 없이 yfinance 직접).
        반환: MultiIndex DataFrame (level0=field, level1=ticker) 또는
              단일 종목이면 flat DataFrame.
        """
        try:
            raw = yf.download(
                tickers,
                start=start.strftime("%Y-%m-%d"),
                end=end.strftime("%Y-%m-%d"),
                auto_adjust=True,
                progress=False,
                threads=True,
            )
            return raw
        except Exception as e:
            log.error("일봉 조회 실패: %s", e)
            return pd.DataFrame()

    def get_prev_day_ohlcv(
        self, tickers: list[str], for_date: date
    ) -> pd.DataFrame:
        """
        for_date 직전 거래일 OHLCV (일봉 최근 1행).
        반환: index=ticker, columns=[high, low, close, volume]
        """
        start = for_date - timedelta(days=10)
        raw = self.get_daily(tickers, start, for_date)
        if raw.empty:
            return pd.DataFrame()

        result = {}
        if isinstance(raw.columns, pd.MultiIndex):
            for t in tickers:
                if t not in raw.columns.get_level_values(1):
                    continue
                df = raw.xs(t, level=1, axis=1).dropna()
                if df.empty:
                    continue
                last = df.iloc[-1]
                result[t] = {
                    "high":   float(last.get("High",   0)),
                    "low":    float(last.get("Low",    0)),
                    "close":  float(last.get("Close",  0)),
                    "volume": float(last.get("Volume", 0)),
                }
        else:
            # 단일 종목 — flat DataFrame
            if tickers and not raw.empty:
                df = raw.dropna()
                if not df.empty:
                    last = df.iloc[-1]
                    t = tickers[0]
                    result[t] = {
                        "high":   float(last.get("High",   last.get("high",   0))),
                        "low":    float(last.get("Low",    last.get("low",    0))),
                        "close":  float(last.get("Close",  last.get("close",  0))),
                        "volume": float(last.get("Volume", last.get("volume", 0))),
                    }
        return pd.DataFrame(result).T if result else pd.DataFrame()
