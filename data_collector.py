"""
data_collector.py
미국(yfinance) + 한국(FinanceDataReader) 주식 데이터 수집 엔진
- SQLite 저장
- 증분 업데이트 지원
- 종목 메타 수집 포함
"""

import sqlite3
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import yfinance as yf
import FinanceDataReader as fdr

# ─────────────────────────────────────────
# 설정
# ─────────────────────────────────────────
DB_PATH = Path("market_data.db")
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
log = logging.getLogger(__name__)

# 기본 미국 종목 (추가 가능)
US_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA",
    "META", "TSLA", "BRK-B", "JPM", "V",
    "SPY", "QQQ", "IWM",  # ETF
]


def get_sp500_tickers() -> list[str]:
    """Wikipedia에서 S&P500 종목 리스트 가져오기 (점 → 대시 변환)"""
    import io
    import requests as req_lib
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    log.info("S&P500 종목 리스트 수집 중... (%s)", url)
    headers = {"User-Agent": "Mozilla/5.0 (compatible; algo-trading-bot/1.0)"}
    resp = req_lib.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    tables = pd.read_html(io.StringIO(resp.text), header=0)
    tickers = tables[0]["Symbol"].tolist()
    # Wikipedia 표기 → yfinance 표기 (BRK.B → BRK-B)
    tickers = [str(t).replace(".", "-") for t in tickers]
    log.info("S&P500 종목 수: %d개", len(tickers))
    return tickers

# ─────────────────────────────────────────
# DB 초기화
# ─────────────────────────────────────────
def init_db(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS us_ohlcv (
            ticker      TEXT    NOT NULL,
            date        TEXT    NOT NULL,
            open        REAL,
            high        REAL,
            low         REAL,
            close       REAL,
            adj_close   REAL,
            volume      INTEGER,
            PRIMARY KEY (ticker, date)
        );

        CREATE TABLE IF NOT EXISTS kr_ohlcv (
            ticker      TEXT    NOT NULL,
            date        TEXT    NOT NULL,
            open        REAL,
            high        REAL,
            low         REAL,
            close       REAL,
            volume      INTEGER,
            market_cap  INTEGER,
            PRIMARY KEY (ticker, date)
        );

        CREATE TABLE IF NOT EXISTS us_meta (
            ticker          TEXT PRIMARY KEY,
            name            TEXT,
            sector          TEXT,
            industry        TEXT,
            country         TEXT,
            market_cap      INTEGER,
            currency        TEXT,
            exchange        TEXT,
            updated_at      TEXT
        );

        CREATE TABLE IF NOT EXISTS kr_meta (
            ticker          TEXT PRIMARY KEY,
            name            TEXT,
            market          TEXT,
            sector          TEXT,
            industry        TEXT,
            market_cap      INTEGER,
            updated_at      TEXT
        );

        CREATE TABLE IF NOT EXISTS collect_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            market      TEXT,
            ticker      TEXT,
            from_date   TEXT,
            to_date     TEXT,
            rows        INTEGER,
            status      TEXT,
            message     TEXT,
            created_at  TEXT DEFAULT (datetime('now','localtime'))
        );
    """)
    conn.commit()

    # 기존 DB 마이그레이션: kr_meta에 market_cap 컬럼이 없으면 추가
    cols = [r[1] for r in conn.execute("PRAGMA table_info(kr_meta)").fetchall()]
    if "market_cap" not in cols:
        conn.execute("ALTER TABLE kr_meta ADD COLUMN market_cap INTEGER")
        conn.commit()
        log.info("kr_meta.market_cap 컬럼 추가 완료")

    log.info("DB 초기화 완료: %s", DB_PATH)


# ─────────────────────────────────────────
# 유틸
# ─────────────────────────────────────────
def get_last_date(conn: sqlite3.Connection, table: str, ticker: str) -> Optional[str]:
    """ticker의 마지막 수집일 반환 (증분 업데이트용)"""
    row = conn.execute(
        f"SELECT MAX(date) FROM {table} WHERE ticker = ?", (ticker,)
    ).fetchone()
    return row[0] if row and row[0] else None


def log_collect(conn, market, ticker, from_date, to_date, rows, status, message=""):
    conn.execute(
        "INSERT INTO collect_log (market,ticker,from_date,to_date,rows,status,message) VALUES (?,?,?,?,?,?,?)",
        (market, ticker, from_date, to_date, rows, status, message),
    )
    conn.commit()


# ─────────────────────────────────────────
# 미국 주식 (yfinance)
# ─────────────────────────────────────────
def fetch_us_meta(conn: sqlite3.Connection, tickers: list[str]):
    log.info("미국 종목 메타 수집 시작: %d개", len(tickers))
    rows = []
    for ticker in tickers:
        try:
            info = yf.Ticker(ticker).info
            rows.append((
                ticker,
                info.get("longName") or info.get("shortName"),
                info.get("sector"),
                info.get("industry"),
                info.get("country"),
                info.get("marketCap"),
                info.get("currency"),
                info.get("exchange"),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ))
        except Exception as e:
            log.warning("US 메타 실패 [%s]: %s", ticker, e)

    conn.executemany(
        """INSERT OR REPLACE INTO us_meta
           (ticker,name,sector,industry,country,market_cap,currency,exchange,updated_at)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        rows,
    )
    conn.commit()
    log.info("미국 메타 저장: %d개", len(rows))


def fetch_us_ohlcv(
    conn: sqlite3.Connection,
    tickers: list[str],
    start: Optional[str] = None,
    end: Optional[str] = None,
):
    end = end or datetime.today().strftime("%Y-%m-%d")

    for ticker in tickers:
        last = get_last_date(conn, "us_ohlcv", ticker)
        if last:
            from_date = (datetime.strptime(last, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            log.info("[US] %s 증분: %s ~ %s", ticker, from_date, end)
        else:
            from_date = start or "2010-01-01"
            log.info("[US] %s 전체: %s ~ %s", ticker, from_date, end)

        if from_date > end:
            log.info("[US] %s 이미 최신", ticker)
            continue

        try:
            df = yf.download(ticker, start=from_date, end=end, progress=False, auto_adjust=False)
            if df.empty:
                log.warning("[US] %s 데이터 없음", ticker)
                log_collect(conn, "US", ticker, from_date, end, 0, "empty")
                continue

            # MultiIndex 처리
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            df = df.rename(columns={
                "Open": "open", "High": "high", "Low": "low",
                "Close": "close", "Adj Close": "adj_close", "Volume": "volume",
            })
            df.index = pd.to_datetime(df.index).strftime("%Y-%m-%d")
            df.index.name = "date"
            df["ticker"] = ticker
            df = df.reset_index()[["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]]

            conn.executemany(
                "INSERT OR REPLACE INTO us_ohlcv VALUES (?,?,?,?,?,?,?,?)",
                df.itertuples(index=False, name=None),
            )
            conn.commit()
            log_collect(conn, "US", ticker, from_date, end, len(df), "ok")
            log.info("[US] %s 저장: %d행", ticker, len(df))

        except Exception as e:
            log.error("[US] %s 오류: %s", ticker, e)
            log_collect(conn, "US", ticker, from_date, end, 0, "error", str(e))


def fetch_us_ohlcv_batch(
    conn: sqlite3.Connection,
    tickers: list[str],
    start: Optional[str] = None,
    end: Optional[str] = None,
    batch_size: int = 100,
):
    """yfinance 배치 다운로드 — S&P500 같은 대량 종목에 사용 (100종목씩 묶어서)"""
    end = end or datetime.today().strftime("%Y-%m-%d")
    start = start or "2010-01-01"

    # 이미 최신인 종목 제외
    to_fetch = []
    for t in tickers:
        last = get_last_date(conn, "us_ohlcv", t)
        next_start = (
            (datetime.strptime(last, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            if last else start
        )
        if next_start <= end:
            to_fetch.append((t, next_start))

    log.info("배치 다운로드 대상: %d종목 (전체 %d, 최신 %d)",
             len(to_fetch), len(tickers), len(tickers) - len(to_fetch))

    for batch_idx in range(0, len(to_fetch), batch_size):
        batch = to_fetch[batch_idx:batch_idx + batch_size]
        batch_tickers = [t for t, _ in batch]
        batch_start   = min(s for _, s in batch)
        n_total = (len(to_fetch) - 1) // batch_size + 1
        log.info("배치 %d/%d: %d종목 (%s~%s)",
                 batch_idx // batch_size + 1, n_total, len(batch_tickers), batch_start, end)

        try:
            raw = yf.download(
                batch_tickers, start=batch_start, end=end,
                auto_adjust=False, progress=False, threads=True,
            )
            if raw is None or raw.empty:
                log.warning("배치 데이터 없음")
                continue

            # 단일 종목이면 columns가 flat
            if not isinstance(raw.columns, pd.MultiIndex):
                raw.columns = pd.MultiIndex.from_product([raw.columns, batch_tickers])

            for ticker, ticker_start in batch:
                try:
                    # 해당 종목 슬라이스
                    if ticker not in raw.columns.get_level_values(1):
                        log.warning("[US-Batch] %s 데이터 없음", ticker)
                        continue
                    df = raw.xs(ticker, level=1, axis=1).copy()
                    df = df[df.index >= ticker_start]
                    if df.empty:
                        continue

                    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
                    df = df.rename(columns={"adj_close": "adj_close"})
                    df.index = pd.to_datetime(df.index).strftime("%Y-%m-%d")
                    df.index.name = "date"
                    df["ticker"] = ticker
                    cols = ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]
                    df = df.reset_index()
                    # 컬럼 보정
                    for c in ["adj_close"]:
                        if c not in df.columns:
                            df[c] = None
                    df = df[[c for c in cols if c in df.columns]]
                    if len(df.columns) < len(cols):
                        for c in cols:
                            if c not in df.columns:
                                df[c] = None
                        df = df[cols]

                    conn.executemany(
                        "INSERT OR REPLACE INTO us_ohlcv VALUES (?,?,?,?,?,?,?,?)",
                        df[cols].itertuples(index=False, name=None),
                    )
                    conn.commit()
                    log_collect(conn, "US", ticker, ticker_start, end, len(df), "ok")
                    log.info("[US-Batch] %s 저장: %d행", ticker, len(df))

                except Exception as e:
                    log.error("[US-Batch] %s 저장 오류: %s", ticker, e)
                    log_collect(conn, "US", ticker, ticker_start, end, 0, "error", str(e))

        except Exception as e:
            log.error("[US-Batch] 배치 오류: %s", e)


# ─────────────────────────────────────────
# 한국 주식 (FinanceDataReader)
# ─────────────────────────────────────────
def get_last_trading_date() -> str:
    """최근 영업일 반환 (주말 스킵, 공휴일 미처리)"""
    dt = datetime.today()
    while dt.weekday() >= 5:  # 5=토, 6=일
        dt -= timedelta(days=1)
    return dt.strftime("%Y-%m-%d")


def fetch_kr_meta(conn: sqlite3.Connection, market: str = "ALL"):
    log.info("한국 종목 메타 수집 시작 (market=%s)", market)
    markets = ["KOSPI", "KOSDAQ"] if market == "ALL" else [market]
    final = []
    updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for mkt in markets:
        try:
            df = fdr.StockListing(mkt)
            if df.empty:
                log.warning("KR 메타 없음: %s", mkt)
                continue
            # FDR 컬럼 정규화 (버전마다 다를 수 있음)
            col_map = {}
            for c in df.columns:
                cl = c.lower()
                if cl in ("symbol", "code"):
                    col_map[c] = "ticker"
                elif cl == "name":
                    col_map[c] = "name"
                elif cl == "sector":
                    col_map[c] = "sector"
                elif cl in ("industry", "dept"):
                    col_map[c] = "industry"
                elif cl == "marcap":
                    col_map[c] = "market_cap"
            df = df.rename(columns=col_map)
            for col in ("ticker", "name", "sector", "industry"):
                if col not in df.columns:
                    df[col] = None
            for _, row in df.iterrows():
                final.append((
                    str(row["ticker"]).zfill(6),
                    row["name"],
                    mkt,
                    row.get("sector"),
                    row.get("industry"),
                    row.get("market_cap"),
                    updated_at,
                ))
            log.info("KR 메타 [%s]: %d개", mkt, len(df))
        except Exception as e:
            log.warning("KR 메타 실패 [%s]: %s", mkt, str(e))

    conn.executemany(
        "INSERT OR REPLACE INTO kr_meta (ticker,name,market,sector,industry,market_cap,updated_at) VALUES (?,?,?,?,?,?,?)",
        final,
    )
    conn.commit()
    log.info("한국 메타 저장: %d개", len(final))


def fetch_kr_ohlcv(
    conn: sqlite3.Connection,
    tickers: Optional[list[str]] = None,
    market: str = "ALL",
    start: Optional[str] = None,
    end: Optional[str] = None,
):
    end_date = end or get_last_trading_date()

    if tickers is None:
        rows = conn.execute("SELECT ticker FROM kr_meta").fetchall()
        if not rows:
            fetch_kr_meta(conn, market)
            rows = conn.execute("SELECT ticker FROM kr_meta").fetchall()
        tickers = [r[0] for r in rows]

    log.info("한국 OHLCV 수집: %d개 종목", len(tickers))

    for ticker in tickers:
        last = get_last_date(conn, "kr_ohlcv", ticker)
        if last:
            from_date = (datetime.strptime(last, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            log.info("[KR] %s 증분: %s ~ %s", ticker, from_date, end_date)
        else:
            from_date = start or "2010-01-01"
            log.info("[KR] %s 전체: %s ~ %s", ticker, from_date, end_date)

        if from_date > end_date:
            log.info("[KR] %s 이미 최신", ticker)
            continue

        try:
            df = fdr.DataReader(ticker, from_date, end_date)
            if df is None or df.empty:
                log.warning("[KR] %s 데이터 없음", ticker)
                log_collect(conn, "KR", ticker, from_date, end_date, 0, "empty")
                continue

            df.columns = [c.lower() for c in df.columns]
            df = df.rename(columns={"adj close": "adj_close"})
            df.index = pd.to_datetime(df.index).strftime("%Y-%m-%d")
            df.index.name = "date"
            df["ticker"] = ticker
            df["market_cap"] = None
            df = df.reset_index()[["ticker", "date", "open", "high", "low", "close", "volume", "market_cap"]]

            conn.executemany(
                "INSERT OR REPLACE INTO kr_ohlcv VALUES (?,?,?,?,?,?,?,?)",
                df.itertuples(index=False, name=None),
            )
            conn.commit()
            log_collect(conn, "KR", ticker, from_date, end_date, len(df), "ok")
            log.info("[KR] %s 저장: %d행", ticker, len(df))

        except Exception as e:
            log.error("[KR] %s 오류: %s", ticker, str(e))
            log_collect(conn, "KR", ticker, from_date, end_date, 0, "error", str(e))


# ─────────────────────────────────────────
# 한국 시가총액 (pykrx)
# ─────────────────────────────────────────
def fetch_kr_market_cap(
    conn: sqlite3.Connection,
    tickers: Optional[list[str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
):
    """
    pykrx로 KR 종목 일별 시가총액 수집 후 kr_ohlcv.market_cap 업데이트.
    tickers=None이면 kr_ohlcv에 있는 전체 종목 대상.
    """
    from pykrx import stock as pykrx_stock

    end_date   = end   or get_last_trading_date()
    start_date = start or "2010-01-01"

    if tickers is None:
        rows = conn.execute("SELECT DISTINCT ticker FROM kr_ohlcv").fetchall()
        tickers = [r[0] for r in rows]

    log.info("[MarketCap] 시가총액 수집 시작: %d종목 (%s ~ %s)", len(tickers), start_date, end_date)

    from_dt = start_date.replace("-", "")
    to_dt   = end_date.replace("-", "")

    for i, ticker in enumerate(tickers, 1):
        try:
            df = pykrx_stock.get_market_cap_by_date(from_dt, to_dt, ticker)
            if df is None or df.empty:
                log.warning("[MarketCap] %s 데이터 없음", ticker)
                continue

            # pykrx 컬럼: 시가총액, 거래량, 거래대금, 상장주수
            if "시가총액" not in df.columns:
                log.warning("[MarketCap] %s 시가총액 컬럼 없음", ticker)
                continue

            df.index = pd.to_datetime(df.index).strftime("%Y-%m-%d")
            rows_to_update = [
                (int(row["시가총액"]), ticker, date)
                for date, row in df.iterrows()
                if pd.notna(row["시가총액"])
            ]

            conn.executemany(
                "UPDATE kr_ohlcv SET market_cap = ? WHERE ticker = ? AND date = ?",
                rows_to_update,
            )
            conn.commit()

            if i % 50 == 0 or i == len(tickers):
                log.info("[MarketCap] %d/%d 완료 (%s, %d행)", i, len(tickers), ticker, len(rows_to_update))

        except Exception as e:
            log.error("[MarketCap] %s 오류: %s", ticker, e)

    log.info("[MarketCap] 시가총액 수집 완료")


# ─────────────────────────────────────────
# 조회 헬퍼
# ─────────────────────────────────────────
def load_ohlcv(
    ticker: str,
    market: str = "US",
    start: Optional[str] = None,
    end: Optional[str] = None,
    db_path: Path = DB_PATH,
) -> pd.DataFrame:
    """저장된 OHLCV 데이터프레임 반환"""
    table = "us_ohlcv" if market == "US" else "kr_ohlcv"
    query = f"SELECT * FROM {table} WHERE ticker = ?"
    params: list = [ticker]
    if start:
        query += " AND date >= ?"
        params.append(start)
    if end:
        query += " AND date <= ?"
        params.append(end)
    query += " ORDER BY date"
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(query, conn, params=params, index_col="date", parse_dates=["date"])
    return df


# ─────────────────────────────────────────
# 엔트리포인트
# ─────────────────────────────────────────
def run(
    us_tickers: Optional[list[str]] = None,
    kr_tickers: Optional[list[str]] = None,
    kr_market: str = "ALL",
    start: Optional[str] = None,
    update_meta: bool = True,
    do_us: bool = True,
    do_kr: bool = True,
    use_sp500: bool = False,
    batch: bool = False,
):
    """
    수집 실행

    Args:
        us_tickers:  미국 종목 리스트 (None이면 기본 US_TICKERS)
        kr_tickers:  한국 종목 리스트 (None이면 kr_market 전체)
        kr_market:   "KOSPI" | "KOSDAQ" | "ALL"
        start:       최초 수집 시작일 "YYYY-MM-DD" (증분이면 무시됨)
        update_meta: 메타 정보 갱신 여부
        do_us:       미국 수집 여부
        do_kr:       한국 수집 여부
        use_sp500:   True이면 Wikipedia에서 S&P500 종목 리스트 사용
        batch:       True이면 배치 다운로드 (대량 종목 권장)
    """
    with sqlite3.connect(DB_PATH) as conn:
        init_db(conn)

        if do_us:
            if use_sp500:
                tickers = get_sp500_tickers()
            else:
                tickers = us_tickers or US_TICKERS
            if update_meta:
                fetch_us_meta(conn, tickers)
            if batch or use_sp500:
                fetch_us_ohlcv_batch(conn, tickers, start=start)
            else:
                fetch_us_ohlcv(conn, tickers, start=start)

        if do_kr:
            if update_meta:
                fetch_kr_meta(conn, kr_market)
            fetch_kr_ohlcv(conn, kr_tickers, market=kr_market, start=start)

    log.info("수집 완료 → %s", DB_PATH.resolve())


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="주식 데이터 수집 엔진")
    parser.add_argument("--mode", default="incremental", choices=["full", "incremental"],
                        help="full=전체재수집 / incremental=증분업데이트(기본)")
    parser.add_argument("--market", default="ALL", choices=["US", "KR", "ALL"],
                        help="수집 대상 시장 (기본: ALL)")
    parser.add_argument("--us", nargs="*", help="미국 종목 (기본: US_TICKERS)")
    parser.add_argument("--kr", nargs="*", help="한국 종목코드 (기본: 전체)")
    parser.add_argument("--kr-market", default="ALL", choices=["KOSPI", "KOSDAQ", "ALL"])
    parser.add_argument("--start", default=None, help="시작일 YYYY-MM-DD (full 모드 시 적용)")
    parser.add_argument("--no-meta",  action="store_true", help="메타 수집 생략")
    parser.add_argument("--sp500",      action="store_true", help="S&P500 전체 종목 수집 (Wikipedia)")
    parser.add_argument("--batch",      action="store_true", help="배치 다운로드 모드 (대량 종목)")
    parser.add_argument("--market-cap", action="store_true", help="KR 시가총액 수집/업데이트 (pykrx)")
    args = parser.parse_args()

    # --mode full 이면 시작일 기본값 설정
    start = args.start
    if args.mode == "full" and not start:
        start = "2010-01-01"

    # --market-cap 단독 실행
    if args.market_cap:
        with sqlite3.connect(DB_PATH) as conn:
            init_db(conn)
            fetch_kr_market_cap(
                conn,
                tickers=args.kr if args.kr else None,
                start=start,
                end=None,
            )
    else:
        # --market 으로 수집 대상 결정
        do_us = args.market in ("US", "ALL")
        do_kr = args.market in ("KR", "ALL")

        run(
            us_tickers=args.us if do_us else [],
            kr_tickers=args.kr if do_kr else None,
            kr_market=args.kr_market,
            start=start,
            update_meta=not args.no_meta,
            do_us=do_us,
            do_kr=do_kr,
            use_sp500=args.sp500,
            batch=args.batch,
        )
