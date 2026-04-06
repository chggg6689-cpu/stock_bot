"""
volatility_trader.py
NASDAQ 고변동성 ORB(Opening Range Breakout) 데이트레이딩

흐름:
  09:20 ET  NASDAQ 전종목 스캔 → 전일 고저폭×거래량 상위 2~3종목 선정
  09:30~35  Opening Range 형성 (첫 5분 고저 기록)
  09:35~10:30  1분봉 체크 (30초 간격)
               조건 충족 시 매수:
                 ① 현재가 > OR 고점 × 1.001 (돌파 확인)
                 ② 현재 1분봉 거래량 > OR 평균 거래량 × 2 (수급 급증)
  장중  TP +5% / SL max(OR 저점, -2.5%) 자동 청산
  15:50 ET  미청산 전량 강제 청산

보수적 필터:
  - 종목당 하루 1회만 진입
  - 10:30 이후 미돌파 시 당일 스킵
  - 최소 주가 $5 이상
  - OR 크기 > 1% (너무 좁은 레인지 제외)

환경변수 (.env):
  ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_BASE_URL
  TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
"""

import os
import time
import logging
import schedule
import math
from datetime import datetime, date, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Optional

import yfinance as yf
import pandas as pd
import requests
import FinanceDataReader as fdr
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.data import StockHistoricalDataClient
from alpaca.data.requests import (
    StockBarsRequest,
    StockLatestTradeRequest,
    StockLatestBarRequest,
)
from alpaca.data.timeframe import TimeFrame

# ─────────────────────────────────────────
# 설정
# ─────────────────────────────────────────
load_dotenv(Path(__file__).parent / ".env")

ALPACA_API_KEY    = os.getenv("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "")
ALPACA_BASE_URL   = os.getenv("ALPACA_BASE_URL", "")
TELEGRAM_TOKEN    = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID  = os.getenv("TELEGRAM_CHAT_ID", "")

ET = ZoneInfo("America/New_York")

# 전략 파라미터
TOP_N              = 3        # 스캔 후보 종목 수
TAKE_PROFIT        = 0.05     # +5%
STOP_LOSS_PCT      = 0.025    # -2.5% (OR 저점보다 넓을 경우 fallback)
MIN_AVG_VOLUME     = 1_000_000
MIN_PRICE          = 5.0      # 최소 주가 ($5)
MIN_OR_RANGE       = 0.01     # 최소 OR 크기 1%
ORB_BUFFER         = 0.001    # 돌파 버퍼 0.1%
VOL_SURGE_MULT     = 2.0      # 거래량 급증 배수
ENTRY_DEADLINE     = "10:30"  # 이후 미진입 시 당일 스킵
FORCE_CLOSE_TIME   = "15:50"  # 강제 청산
SCAN_TIME          = "09:20"
OR_START           = "09:30"
OR_END             = "09:35"  # OR 형성 완료
MONITOR_INTERVAL   = 30       # 초

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────
# Telegram
# ─────────────────────────────────────────
class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self.token   = token
        self.chat_id = chat_id
        self.enabled = bool(token and chat_id)

    def send(self, msg: str) -> bool:
        if not self.enabled:
            return False
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{self.token}/sendMessage",
                json={"chat_id": self.chat_id, "text": msg, "parse_mode": "Markdown"},
                timeout=10,
            )
            resp.raise_for_status()
            return True
        except Exception as e:
            log.error("[Telegram] 실패: %s", e)
            return False


# ─────────────────────────────────────────
# NASDAQ 스캐너 (전일 기준)
# ─────────────────────────────────────────
class NasdaqScanner:
    _universe: list[str] = []
    _cache_date: Optional[date] = None

    def get_universe(self) -> list[str]:
        today = date.today()
        if self._cache_date == today and self._universe:
            return self._universe
        log.info("[Scanner] NASDAQ 유니버스 로딩...")
        try:
            df  = fdr.StockListing("NASDAQ")
            col = next((c for c in df.columns if c.lower() in ("symbol", "code")), None)
            tickers = [str(t) for t in df[col].tolist()
                       if str(t).isalpha() and 1 < len(t) <= 5]
            NasdaqScanner._universe    = tickers
            NasdaqScanner._cache_date  = today
            log.info("[Scanner] %d종목 로드", len(tickers))
            return tickers
        except Exception as e:
            log.error("[Scanner] FDR 실패: %s", e)
            return [
                "NVDA","AMD","META","TSLA","AAPL","AMZN","MSFT","NFLX","GOOGL","AVGO",
                "MRVL","SMCI","PLTR","CRWD","DDOG","NET","COIN","HOOD","MSTR","IONQ",
            ]

    def scan(self, top_n: int = TOP_N) -> list[dict]:
        """전일 고저폭(%) × log10(거래량) 스코어 상위 종목"""
        universe = self.get_universe()
        end   = datetime.now(ET).strftime("%Y-%m-%d")
        start = (datetime.now(ET) - timedelta(days=5)).strftime("%Y-%m-%d")

        results: list[dict] = []
        batch_size = 200
        for i in range(0, len(universe), batch_size):
            batch = universe[i:i + batch_size]
            try:
                raw = yf.download(
                    batch, start=start, end=end,
                    auto_adjust=True, progress=False, threads=True,
                )
                if raw.empty or not isinstance(raw.columns, pd.MultiIndex):
                    continue
                for t in batch:
                    try:
                        if t not in raw.columns.get_level_values(1):
                            continue
                        df = raw.xs(t, level=1, axis=1).dropna()
                        if len(df) < 2:
                            continue
                        row = df.iloc[-1]
                        vol = float(row.get("Volume", 0))
                        hi  = float(row.get("High",  0))
                        lo  = float(row.get("Low",   1))
                        cl  = float(row.get("Close", 1))
                        if vol < MIN_AVG_VOLUME or cl < MIN_PRICE or lo <= 0:
                            continue
                        hl_pct = (hi - lo) / cl
                        score  = hl_pct * math.log10(max(vol, 1))
                        results.append({
                            "ticker": t, "hl_pct": hl_pct,
                            "volume": int(vol), "close": cl, "score": score,
                        })
                    except Exception:
                        continue
            except Exception as e:
                log.warning("[Scanner] 배치 오류: %s", e)

        if not results:
            return []
        top = (pd.DataFrame(results)
               .sort_values("score", ascending=False)
               .head(top_n)
               .to_dict("records"))
        log.info("[Scanner] 선정:")
        for r in top:
            log.info("  %s  HL=%s%%  Vol=%s  Score=%.3f",
                     r["ticker"], f"{r['hl_pct']*100:.1f}",
                     f"{r['volume']:,}", r["score"])
        return top


# ─────────────────────────────────────────
# Alpaca 클라이언트
# ─────────────────────────────────────────
class AlpacaClient:
    def __init__(self):
        self.trader = TradingClient(
            ALPACA_API_KEY, ALPACA_SECRET_KEY,
            paper=True, url_override=ALPACA_BASE_URL or None,
        )
        self.data = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

    def get_equity(self) -> float:
        return float(self.trader.get_account().equity)

    def get_positions(self) -> dict[str, dict]:
        return {
            p.symbol: {"qty": float(p.qty), "avg_entry": float(p.avg_entry_price)}
            for p in self.trader.get_all_positions()
        }

    def get_latest_bar(self, symbol: str) -> Optional[dict]:
        """최신 1분봉 (시가·고가·저가·종가·거래량)"""
        try:
            bars = self.data.get_stock_latest_bar(
                StockLatestBarRequest(symbol_or_symbols=symbol)
            )
            b = bars[symbol]
            return {"open": b.open, "high": b.high, "low": b.low,
                    "close": b.close, "volume": b.volume}
        except Exception as e:
            log.warning("[Bar] %s 조회 실패: %s", symbol, e)
            return None

    def get_bars(self, symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
        """지정 기간 1분봉 DataFrame"""
        try:
            req  = StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=TimeFrame.Minute,
                start=start, end=end,
            )
            bars = self.data.get_stock_bars(req).df
            if isinstance(bars.index, pd.MultiIndex):
                bars = bars.xs(symbol, level="symbol")
            return bars
        except Exception as e:
            log.warning("[Bars] %s 조회 실패: %s", symbol, e)
            return pd.DataFrame()

    def get_price(self, symbol: str) -> Optional[float]:
        try:
            trades = self.data.get_stock_latest_trade(
                StockLatestTradeRequest(symbol_or_symbols=symbol)
            )
            return float(trades[symbol].price)
        except Exception as e:
            log.warning("[Price] %s 실패: %s", symbol, e)
            return None

    def market_order(self, symbol: str, qty: int, side: OrderSide) -> bool:
        try:
            req   = MarketOrderRequest(
                symbol=symbol, qty=qty, side=side, time_in_force=TimeInForce.DAY
            )
            order = self.trader.submit_order(req)
            log.info("[Order] %s %s %d주 id=%s", side.value.upper(), symbol, qty, order.id)
            return True
        except Exception as e:
            log.error("[Order] %s %s 실패: %s", side.value.upper(), symbol, e)
            return False

    def close_all(self):
        try:
            self.trader.close_all_positions(cancel_orders=True)
        except Exception as e:
            log.error("[CloseAll] %s", e)


# ─────────────────────────────────────────
# Opening Range 관리
# ─────────────────────────────────────────
class OpeningRange:
    """종목별 OR 고저 + 기준 거래량 보관"""

    def __init__(self):
        self._ranges: dict[str, dict] = {}

    def build(self, symbol: str, client: AlpacaClient) -> bool:
        """09:30~09:35 1분봉으로 OR 구성"""
        today = datetime.now(ET).date()
        start = datetime(today.year, today.month, today.day,  9, 30, tzinfo=ET)
        end   = datetime(today.year, today.month, today.day,  9, 35, tzinfo=ET)

        bars = client.get_bars(symbol, start, end)
        if bars.empty:
            log.warning("[OR] %s 바 없음 — OR 구성 실패", symbol)
            return False

        or_high   = float(bars["high"].max())
        or_low    = float(bars["low"].min())
        or_range  = (or_high - or_low) / or_low if or_low > 0 else 0
        avg_vol   = float(bars["volume"].mean())

        if or_range < MIN_OR_RANGE:
            log.info("[OR] %s OR 범위 %.2f%% < %.0f%% — 스킵",
                     symbol, or_range * 100, MIN_OR_RANGE * 100)
            return False

        self._ranges[symbol] = {
            "high": or_high, "low": or_low,
            "range_pct": or_range, "avg_vol": avg_vol,
        }
        log.info("[OR] %s H=%.2f L=%.2f Range=%.2f%% AvgVol=%s",
                 symbol, or_high, or_low,
                 or_range * 100, f"{avg_vol:,.0f}")
        return True

    def get(self, symbol: str) -> Optional[dict]:
        return self._ranges.get(symbol)

    def is_breakout(self, symbol: str, bar: dict) -> bool:
        """
        돌파 조건:
          ① 현재 종가 > OR 고점 × (1 + buffer)
          ② 현재 1분봉 거래량 > OR 평균 거래량 × VOL_SURGE_MULT
        """
        r = self.get(symbol)
        if r is None:
            return False
        price_ok  = bar["close"] > r["high"] * (1 + ORB_BUFFER)
        volume_ok = bar["volume"] > r["avg_vol"] * VOL_SURGE_MULT
        log.info("[ORB] %s 가격돌파=%s (%.2f>%.2f) 거래량급증=%s (%s>%s)",
                 symbol,
                 price_ok, bar["close"], r["high"] * (1 + ORB_BUFFER),
                 volume_ok,
                 f"{bar['volume']:,.0f}", f"{r['avg_vol']*VOL_SURGE_MULT:,.0f}")
        return price_ok and volume_ok

    def get_sl_price(self, symbol: str, entry_price: float) -> float:
        """SL = OR 저점 vs 진입가 -2.5% 중 높은 쪽 (더 타이트한 손절)"""
        r = self.get(symbol)
        or_low_sl = r["low"] if r else entry_price * (1 - STOP_LOSS_PCT)
        pct_sl    = entry_price * (1 - STOP_LOSS_PCT)
        return max(or_low_sl, pct_sl)


# ─────────────────────────────────────────
# ORB 데이트레이더
# ─────────────────────────────────────────
class ORBDayTrader:
    def __init__(self):
        if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
            raise ValueError("Alpaca API 키 설정 필요")

        self.client    = AlpacaClient()
        self.notifier  = TelegramNotifier(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
        self.scanner   = NasdaqScanner()
        self.or_mgr    = OpeningRange()

        self._candidates: list[dict] = []
        self._entered:   dict[str, float] = {}  # ticker → avg_entry_price
        self._sl_prices: dict[str, float] = {}  # ticker → SL 가격
        self._skipped:   set[str]         = set()   # 당일 스킵 확정 종목
        self._monitoring = False

    # ── 09:20 스캔 ───────────────────────
    def on_scan(self):
        now = datetime.now(ET)
        if now.weekday() >= 5:
            return
        self._reset()
        self._candidates = self.scanner.scan(top_n=TOP_N)
        if self._candidates:
            lines = "\n".join(
                f"  `{c['ticker']}` HL={c['hl_pct']:.1%}  Vol={c['volume']:,}"
                for c in self._candidates
            )
            self.notifier.send(
                f"🔍 *오늘의 후보 ({len(self._candidates)}종목)*\n{lines}\n"
                f"09:35 돌파 감시 시작 예정"
            )
        else:
            self.notifier.send("⚠️ 오늘 후보 없음")

    # ── 09:35 OR 구성 + 감시 시작 ────────
    def on_or_complete(self):
        now = datetime.now(ET)
        if now.weekday() >= 5 or not self._candidates:
            return

        log.info("[ORB] OR 구성 시작")
        valid = []
        for c in self._candidates:
            if self.or_mgr.build(c["ticker"], self.client):
                valid.append(c["ticker"])
            else:
                self._skipped.add(c["ticker"])

        if valid:
            r_info = []
            for t in valid:
                r = self.or_mgr.get(t)
                r_info.append(
                    f"  `{t}` H=${r['high']:.2f} L=${r['low']:.2f} Range={r['range_pct']:.1%}"
                )
            self.notifier.send(
                f"📐 *Opening Range 확정*\n"
                + "\n".join(r_info)
                + f"\n\n돌파 감시 시작 (마감: {ENTRY_DEADLINE} ET)"
            )
            self._monitoring = True
        else:
            self.notifier.send("⚠️ 유효한 OR 없음 — 오늘 스킵")

    # ── 30초마다: 돌파 체크 + TP/SL 체크 ─
    def on_tick(self):
        if not self._monitoring:
            return
        now = datetime.now(ET)
        if now.weekday() >= 5:
            return

        # 진입 마감 체크
        deadline = now.replace(
            hour=int(ENTRY_DEADLINE.split(":")[0]),
            minute=int(ENTRY_DEADLINE.split(":")[1]),
            second=0, microsecond=0,
        )

        # ── TP/SL 체크 (이미 진입한 종목) ──
        positions = self.client.get_positions()
        for ticker, pos in positions.items():
            if ticker not in self._entered:
                continue
            price = self.client.get_price(ticker)
            if price is None:
                continue
            sl = self._sl_prices.get(ticker, self._entered[ticker] * (1 - STOP_LOSS_PCT))
            tp = self._entered[ticker] * (1 + TAKE_PROFIT)
            pnl = (price - self._entered[ticker]) / self._entered[ticker]

            if price >= tp:
                self._exit(ticker, int(pos["qty"]), price, "TP", pnl)
            elif price <= sl:
                self._exit(ticker, int(pos["qty"]), price, "SL", pnl)

        # ── 돌파 체크 (미진입 종목) ──────
        for c in self._candidates:
            ticker = c["ticker"]
            if ticker in self._entered or ticker in self._skipped:
                continue

            # 진입 마감 초과 → 스킵
            if now >= deadline:
                log.info("[ORB] %s %s 마감 초과 — 당일 스킵", ticker, ENTRY_DEADLINE)
                self._skipped.add(ticker)
                self.notifier.send(f"⏰ `{ticker}` {ENTRY_DEADLINE} 마감 — 오늘 패스")
                continue

            bar = self.client.get_latest_bar(ticker)
            if bar is None:
                continue

            if self.or_mgr.is_breakout(ticker, bar):
                self._enter(ticker, bar["close"], positions)

    def _enter(self, ticker: str, price: float, positions: dict):
        equity    = self.client.get_equity()
        n_remain  = max(len(self._candidates) - len(self._entered) - len(self._skipped), 1)
        alloc     = equity / n_remain
        qty       = int(alloc / price)
        if qty <= 0:
            log.warning("[Entry] %s qty=0 스킵 (price=%.2f alloc=%.0f)", ticker, price, alloc)
            return

        r  = self.or_mgr.get(ticker)
        sl = self.or_mgr.get_sl_price(ticker, price)

        log.info("[Entry] %s %d주 @ %.2f | TP=%.2f SL=%.2f",
                 ticker, qty, price,
                 price * (1 + TAKE_PROFIT), sl)

        if self.client.market_order(ticker, qty, OrderSide.BUY):
            self._entered[ticker]   = price
            self._sl_prices[ticker] = sl
            self.notifier.send(
                f"🟢 *ORB 돌파 매수* `{ticker}`\n"
                f"   진입: `${price:.2f}` | {qty}주\n"
                f"   TP: `${price*(1+TAKE_PROFIT):.2f}` (+{TAKE_PROFIT:.0%})\n"
                f"   SL: `${sl:.2f}` (OR저점 또는 -{STOP_LOSS_PCT:.1%})\n"
                f"   OR 범위: ${r['low']:.2f} ~ ${r['high']:.2f}"
            )

    def _exit(self, ticker: str, qty: int, price: float, reason: str, pnl: float):
        icon = "✅" if reason == "TP" else "🛑"
        if self.client.market_order(ticker, qty, OrderSide.SELL):
            self._skipped.add(ticker)
            self.notifier.send(
                f"{icon} *{reason} 청산* `{ticker}`\n"
                f"   청산가: `${price:.2f}` | 수익률: `{pnl:+.2%}`"
            )

    # ── 15:50 강제 청산 ──────────────────
    def on_force_close(self):
        now = datetime.now(ET)
        if now.weekday() >= 5:
            return
        self._monitoring = False
        positions = self.client.get_positions()

        if not positions:
            log.info("[EOD] 포지션 없음")
            self.notifier.send(
                f"📋 *오늘 거래 완료* (미청산 없음)\n"
                f"💰 계좌: `${self.client.get_equity():,.0f}`"
            )
            return

        log.info("[EOD] 강제 청산: %d종목", len(positions))
        self.notifier.send(f"⏰ *15:50 강제 청산* {len(positions)}종목")
        self.client.close_all()
        time.sleep(3)

        equity = self.client.get_equity()
        lines  = []
        for ticker, pos in positions.items():
            price = self.client.get_price(ticker)
            avg   = self._entered.get(ticker, pos["avg_entry"])
            pnl   = (price - avg) / avg if price else 0
            lines.append(f"  `{ticker}` {pnl:+.2%}")

        self.notifier.send(
            f"📋 *오늘 결과*\n" + "\n".join(lines) +
            f"\n💰 최종 계좌: `${equity:,.0f}`"
        )

    def _reset(self):
        self._candidates = []
        self._entered    = {}
        self._sl_prices  = {}
        self._skipped    = set()
        self._monitoring = False
        self.or_mgr      = OpeningRange()


# ─────────────────────────────────────────
# 스케줄러
# ─────────────────────────────────────────
def run_scheduler(trader: ORBDayTrader):
    equity = trader.client.get_equity()
    trader.notifier.send(
        f"🤖 *ORB DayTrader 시작*\n"
        f"전략: NASDAQ ORB (Opening Range Breakout)\n"
        f"TP: +{TAKE_PROFIT:.0%} | SL: OR저점 or -{STOP_LOSS_PCT:.1%}\n"
        f"진입 마감: {ENTRY_DEADLINE} ET | 강제 청산: {FORCE_CLOSE_TIME} ET\n"
        f"계좌: `${equity:,.0f}`"
    )

    schedule.every().day.at(SCAN_TIME).do(trader.on_scan)
    schedule.every().day.at(OR_END).do(trader.on_or_complete)
    schedule.every(MONITOR_INTERVAL).seconds.do(trader.on_tick)
    schedule.every().day.at(FORCE_CLOSE_TIME).do(trader.on_force_close)

    log.info("스케줄러 시작")
    while True:
        schedule.run_pending()
        time.sleep(1)


# ─────────────────────────────────────────
# CLI
# ─────────────────────────────────────────
if __name__ == "__main__":
    import argparse, sys

    parser = argparse.ArgumentParser(description="NASDAQ ORB 데이트레이딩")
    parser.add_argument("--dry-run", action="store_true", help="스캔만 실행 (주문 없음)")
    parser.add_argument("--top-n",  type=int, default=TOP_N)
    args = parser.parse_args()

    if args.dry_run:
        scanner    = NasdaqScanner()
        candidates = scanner.scan(top_n=args.top_n)
        print(f"\n=== Dry Run: 오늘의 ORB 후보 ({len(candidates)}종목) ===")
        for c in candidates:
            print(f"  {c['ticker']:<8} HL={c['hl_pct']:.1%}  "
                  f"Vol={c['volume']:>13,}  Score={c['score']:.3f}")
        sys.exit(0)

    trader = ORBDayTrader()
    run_scheduler(trader)
