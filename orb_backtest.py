"""
orb_backtest.py
ORB 전략 최근 7일 백테스트 (yfinance 1분봉)

흐름:
  각 거래일마다:
    1. 전일 OHLCV → 고저폭×거래량 상위 TOP_N 후보 선정
    2. 해당일 1분봉 조회
    3. 09:30~09:35 OR 형성
    4. 09:35~10:30 돌파 시뮬레이션
    5. TP/SL/EOD 청산 기록
"""

import math
import warnings
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────
# 파라미터 (volatility_trader.py와 동일)
# ─────────────────────────────────────────
TOP_N           = 3
TAKE_PROFIT     = 0.07
STOP_LOSS_PCT   = 0.035
MIN_PRICE       = 5.0
MIN_AVG_VOLUME  = 1_000_000
MIN_OR_RANGE    = 0.0     # OR 범위 필터 해제
ORB_BUFFER      = 0.0     # 돌파 버퍼 해제
VOL_SURGE_MULT  = 1.0     # 거래량 조건 해제 (1.0 = OR 평균 이상이면 통과)
ENTRY_DEADLINE  = (13, 0) # 진입 마감 13:00으로 연장
FORCE_CLOSE     = (15, 30)   # ET 15:30 (마감 변동성 회피)
ET              = ZoneInfo("America/New_York")

# 백테스트 유니버스 — NASDAQ 100 + 고변동성 ETF (유동성 검증된 종목만)
UNIVERSE = [
    # NASDAQ 100 대형주
    "NVDA","AMD","META","TSLA","AAPL","AMZN","MSFT","NFLX","GOOGL","AVGO",
    "MRVL","SMCI","PLTR","CRWD","DDOG","NET","INTC","QCOM","AMAT","MU",
    "LRCX","KLAC","MELI","PYPL","INTU","ISRG","PANW","CDNS","FTNT","ABNB",
    "ZS","REGN","VRTX","GILD","MRNA","SNPS","ADSK","TEAM","WDAY","OKTA",
    "ZM","DOCU","BILL","HUBS","SMAR","SHOP","SE","BIDU","JD","PDD",
    "COIN","HOOD","SOFI","AFRM","UPST","RIVN","NIO","LCID","XPEV","LI",
    # 고변동성 레버리지 ETF (유동성 높음)
    "SOXL","TQQQ","UVXY","SQQQ","SPXL","TECL","FNGU","LABU","DPST","NAIL",
    "TNA","CURE","WANT","HIBL","WEBL","BULZ","RETL","PILL","DFEN","UDOW",
    # 고변동성 개별주 (유동성 충분)
    "SOUN","RKLB","JOBY","ACHR","WOLF","CELH","BYND","PLUG","CHPT","BE",
]


# ─────────────────────────────────────────
# 유틸
# ─────────────────────────────────────────
def get_trading_days(n: int = 8) -> list[date]:
    """최근 n 영업일 목록 (오늘 포함, 내림차순)"""
    days = []
    d = date.today()
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d)
        d -= timedelta(days=1)
    return days


def get_prev_day_ohlcv(tickers: list[str], for_date: date) -> pd.DataFrame:
    """for_date 기준 전일 OHLCV (일봉)"""
    end   = for_date
    start = for_date - timedelta(days=10)
    raw   = yf.download(
        tickers, start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"),
        auto_adjust=True, progress=False, threads=True,
    )
    if raw.empty:
        return pd.DataFrame()
    # 가장 최근 거래일 데이터
    if isinstance(raw.columns, pd.MultiIndex):
        result = {}
        for t in tickers:
            if t not in raw.columns.get_level_values(1):
                continue
            df = raw.xs(t, level=1, axis=1).dropna()
            if df.empty:
                continue
            last = df.iloc[-1]
            result[t] = {
                "high": float(last.get("High", 0)),
                "low":  float(last.get("Low",  0)),
                "close":float(last.get("Close",0)),
                "volume": float(last.get("Volume", 0)),
            }
        return pd.DataFrame(result).T
    return pd.DataFrame()


def select_candidates(prev_ohlcv: pd.DataFrame, top_n: int = TOP_N) -> list[str]:
    """전일 고저폭 × log(거래량) 상위 종목"""
    scores = []
    for t, row in prev_ohlcv.iterrows():
        vol = row["volume"]
        cl  = row["close"]
        hi  = row["high"]
        lo  = row["low"]
        if vol < MIN_AVG_VOLUME or cl < MIN_PRICE or lo <= 0:
            continue
        hl_pct = (hi - lo) / cl
        score  = hl_pct * math.log10(max(vol, 1))
        scores.append((t, hl_pct, vol, score))
    scores.sort(key=lambda x: x[3], reverse=True)
    return [s[0] for s in scores[:top_n]]


def get_intraday_1min(tickers: list[str], for_date: date) -> dict[str, pd.DataFrame]:
    """for_date 당일 1분봉 (ET 기준)"""
    result = {}
    for t in tickers:
        try:
            df = yf.download(
                t,
                start=for_date.strftime("%Y-%m-%d"),
                end=(for_date + timedelta(days=1)).strftime("%Y-%m-%d"),
                interval="1m",
                auto_adjust=True,
                progress=False,
            )
            if df.empty:
                continue
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df.index = pd.to_datetime(df.index).tz_convert(ET)
            result[t] = df
        except Exception as e:
            print(f"  [warn] {t} 1분봉 조회 실패: {e}")
    return result


# ─────────────────────────────────────────
# 단일 종목 ORB 시뮬레이션
# ─────────────────────────────────────────
def simulate_orb(ticker: str, bars: pd.DataFrame, trade_date: date) -> dict | None:
    """
    bars: ET 기준 1분봉 DataFrame
    반환: 거래 결과 dict or None (미진입)
    """
    # 당일 바만 필터
    day_bars = bars[bars.index.date == trade_date].copy()
    if day_bars.empty:
        return None

    # ── Opening Range (09:30~09:35) ──────
    or_bars = day_bars.between_time("09:30", "09:34")
    if or_bars.empty:
        return None

    or_high  = float(or_bars["High"].max())
    or_low   = float(or_bars["Low"].min())
    or_range = (or_high - or_low) / or_low if or_low > 0 else 0
    # 09:30 첫 바는 오픈 스파이크로 항상 비정상적으로 큼 → 제외하고 평균
    vol_ref  = or_bars.iloc[1:] if len(or_bars) > 1 else or_bars
    avg_vol  = float(vol_ref["Volume"].mean())

    if or_range < MIN_OR_RANGE:
        return {"ticker": ticker, "date": trade_date,
                "result": "SKIP_OR", "reason": f"OR 범위 {or_range:.2%} < {MIN_OR_RANGE:.0%}",
                "pnl_pct": 0.0, "entry": None}

    # ── 돌파 구간 (09:35~ENTRY_DEADLINE) ──────────
    deadline_str = f"{ENTRY_DEADLINE[0]:02d}:{ENTRY_DEADLINE[1]:02d}"
    watch_bars = day_bars.between_time("09:35", deadline_str)
    entry_price  = None
    entry_time   = None

    for ts, row in watch_bars.iterrows():
        price    = float(row["Close"])
        vol_bar  = float(row["Volume"])
        price_ok = price > or_high * (1 + ORB_BUFFER)
        vol_ok   = vol_bar > avg_vol * VOL_SURGE_MULT

        if price_ok and vol_ok:
            entry_price = price
            entry_time  = ts
            break

    if entry_price is None:
        return {"ticker": ticker, "date": trade_date,
                "result": "NO_ENTRY", "reason": f"{deadline_str}까지 돌파 없음",
                "pnl_pct": 0.0, "entry": None}

    # ── TP / SL / EOD 추적 ───────────────
    tp_price    = entry_price * (1 + TAKE_PROFIT)
    sl_price    = max(or_low, entry_price * (1 - STOP_LOSS_PCT))
    after_entry = day_bars[day_bars.index > entry_time]

    exit_price  = None
    exit_reason = "EOD"

    for ts, row in after_entry.iterrows():
        # EOD 강제 청산
        if ts.hour > FORCE_CLOSE[0] or (ts.hour == FORCE_CLOSE[0] and ts.minute >= FORCE_CLOSE[1]):
            exit_price  = float(row["Close"])
            exit_reason = "EOD"
            break
        hi = float(row["High"])
        lo = float(row["Low"])
        if hi >= tp_price:
            exit_price  = tp_price
            exit_reason = "TP"
            break
        if lo <= sl_price:
            exit_price  = sl_price
            exit_reason = "SL"
            break

    if exit_price is None:
        # 마지막 바 종가
        last = day_bars.iloc[-1]
        exit_price  = float(last["Close"])
        exit_reason = "EOD"

    pnl_pct = (exit_price - entry_price) / entry_price

    return {
        "ticker":     ticker,
        "date":       trade_date,
        "result":     exit_reason,
        "or_high":    or_high,
        "or_low":     or_low,
        "or_range":   or_range,
        "entry":      entry_price,
        "entry_time": entry_time,
        "exit":       exit_price,
        "pnl_pct":    pnl_pct,
        "reason":     "",
    }


# ─────────────────────────────────────────
# 전체 백테스트
# ─────────────────────────────────────────
def run_backtest(days: int = 7, top_n: int = TOP_N):
    trading_days = get_trading_days(days + 1)  # +1 = 전일 데이터용 여유
    test_days    = trading_days[:days]          # 최근 days일 (테스트 대상)

    all_trades: list[dict] = []

    for test_date in reversed(test_days):
        print(f"\n{'='*60}")
        print(f"  {test_date.strftime('%Y-%m-%d (%a)')}")
        print(f"{'='*60}")

        # 1. 전일 데이터로 후보 선정
        prev_ohlcv   = get_prev_day_ohlcv(UNIVERSE, test_date)
        if prev_ohlcv.empty:
            print("  전일 데이터 없음 - 스킵")
            continue

        candidates = select_candidates(prev_ohlcv, top_n)
        if not candidates:
            print("  후보 없음 - 스킵")
            continue
        print(f"  후보: {candidates}")

        # 2. 1분봉 조회
        intraday = get_intraday_1min(candidates, test_date)

        # 3. 종목별 ORB 시뮬레이션
        for ticker in candidates:
            if ticker not in intraday:
                print(f"  {ticker}: 1분봉 없음")
                continue
            trade = simulate_orb(ticker, intraday[ticker], test_date)
            if trade is None:
                continue
            all_trades.append(trade)

            result = trade["result"]
            pnl    = trade["pnl_pct"]
            icon   = "[TP]" if result == "TP" else ("[SL]" if result == "SL" else ("[TR]" if result == "TRAIL" else ("[EOD]" if result == "EOD" else "[--]")))
            if trade["entry"]:
                print(f"  {icon} {ticker:<6} 진입=${trade['entry']:.2f}"
                      f"  OR={trade['or_range']:.1%}"
                      f"  [{result}] PnL={pnl:+.2%}")
            else:
                print(f"  -- {ticker:<6} {trade['reason']}")

    # ─────────────────────────────────────
    # 결과 요약
    # ─────────────────────────────────────
    entered = [t for t in all_trades if t["entry"] is not None]
    skipped = [t for t in all_trades if t["entry"] is None]

    print(f"\n{'='*60}")
    print(f"  백테스트 결과 요약 (최근 {days}일)")
    print(f"{'='*60}")
    print(f"  총 시도:    {len(all_trades)}건")
    print(f"  실제 진입:  {len(entered)}건")
    print(f"  미진입:     {len(skipped)}건 (OR 불성립 / 10:30 미돌파)")

    if entered:
        pnls    = [t["pnl_pct"] for t in entered]
        wins    = [p for p in pnls if p > 0]
        losses  = [p for p in pnls if p <= 0]
        tp_cnt  = sum(1 for t in entered if t["result"] == "TP")
        sl_cnt  = sum(1 for t in entered if t["result"] == "SL")
        eod_cnt = sum(1 for t in entered if t["result"] == "EOD")
        avg_pnl = sum(pnls) / len(pnls)
        total   = sum(pnls)   # 단순합 (동일비중 가정)

        print(f"\n  진입 건 통계:")
        print(f"  승률:       {len(wins)/len(pnls):.1%}  ({len(wins)}승 {len(losses)}패)")
        print(f"  평균 수익률: {avg_pnl:+.2%}")
        print(f"  누적 수익률: {total:+.2%}  (동일비중 합산)")
        print(f"  최대 수익:  {max(pnls):+.2%}")
        print(f"  최대 손실:  {min(pnls):+.2%}")
        print(f"\n  청산 유형:")
        print(f"  TP 달성:   {tp_cnt}건")
        print(f"  SL 손절:   {sl_cnt}건")
        print(f"  EOD 강제:  {eod_cnt}건")

        print(f"\n  거래 상세:")
        print(f"  {'날짜':<12} {'종목':<8} {'진입':<8} {'청산':<8} {'결과':<6} {'PnL':>8}")
        print(f"  {'-'*56}")
        for t in entered:
            icon = "[TP]" if t["result"] == "TP" else ("[SL]" if t["result"] == "SL" else ("[TR]" if t["result"] == "TRAIL" else "[EOD]"))
            print(f"  {str(t['date']):<12} {t['ticker']:<8} "
                  f"${t['entry']:<7.2f} ${t['exit']:<7.2f} "
                  f"{icon}{t['result']:<4} {t['pnl_pct']:>+.2%}")
    else:
        print("\n  진입 거래 없음 (모든 날 미돌파 또는 OR 불성립)")

    print(f"{'='*60}")
    return all_trades


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="ORB 전략 백테스트 (최근 7일)")
    parser.add_argument("--days",  type=int, default=7,    help="백테스트 일수 (기본 7)")
    parser.add_argument("--top-n", type=int, default=TOP_N, help=f"종목 수 (기본 {TOP_N})")
    args = parser.parse_args()
    run_backtest(days=args.days, top_n=args.top_n)
