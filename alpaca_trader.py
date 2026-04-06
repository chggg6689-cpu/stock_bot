"""
alpaca_trader.py
Alpaca Paper Trading — Momentum-60d 전략

- 매주 월요일 09:35 ET 리밸런싱
- 상위 5종목 동일비중 (각 20%)
- Telegram 알림 (진입/청산/오류/주간 리포트)

환경변수 설정 (.env 또는 shell):
    ALPACA_API_KEY     = <Alpaca Paper API Key>
    ALPACA_SECRET_KEY  = <Alpaca Paper Secret Key>
    TELEGRAM_TOKEN     = <Telegram Bot Token>
    TELEGRAM_CHAT_ID   = <Telegram Chat ID>
"""

import os
import time
import logging
import schedule
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Optional

import io
import numpy as np
import pandas as pd
import requests
import yfinance as yf
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

try:
    from data_collector import get_sp500_tickers as _get_sp500
except ImportError:
    _get_sp500 = None


def get_sp500_with_sectors() -> tuple[list[str], dict[str, str]]:
    """S&P500 종목 리스트 + GICS 섹터 맵 반환 (Wikipedia 파싱)"""
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; algo-trading-bot/1.0)"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    df = pd.read_html(io.StringIO(resp.text), header=0)[0]
    df["Symbol"] = df["Symbol"].str.replace(".", "-", regex=False)
    tickers = df["Symbol"].tolist()
    sector_map = dict(zip(df["Symbol"], df["GICS Sector"]))
    log.info("S&P500 종목 수: %d개, 섹터 수: %d개", len(tickers), df["GICS Sector"].nunique())
    return tickers, sector_map


def get_sp500_tickers() -> list[str]:
    tickers, _ = get_sp500_with_sectors()
    return tickers

# .env 자동 로드 (파일 위치: 스크립트와 같은 디렉토리)
load_dotenv(Path(__file__).parent / ".env")

# ─────────────────────────────────────────
# 설정
# ─────────────────────────────────────────
ALPACA_API_KEY    = os.getenv("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "")
ALPACA_BASE_URL   = os.getenv("ALPACA_BASE_URL", "")   # paper: https://paper-api.alpaca.markets/v2
TELEGRAM_TOKEN    = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID  = os.getenv("TELEGRAM_CHAT_ID", "")

# UNIVERSE는 런타임에 get_sp500_tickers()로 동적 로드
# yfinance 표기 → Alpaca 표기 변환
TICKER_MAP = {"BRK-B": "BRK.B"}

TOP_K          = 10
LOOKBACK       = 60      # 모멘텀 lookback (영업일)
REBAL_TIME     = "09:35" # ET 기준 (시장 오픈 5분 후)
STOP_LOSS_PCT  = 0.15    # 포트폴리오 고점 대비 -15% 손절
ET             = ZoneInfo("America/New_York")
TOP_SECTORS    = 4       # 상위 N개 섹터만 종목 선택

# GICS 섹터 → 섹터 ETF 매핑
SECTOR_ETF_MAP: dict[str, str] = {
    "Communication Services": "XLC",
    "Consumer Discretionary": "XLY",
    "Consumer Staples":       "XLP",
    "Energy":                 "XLE",
    "Financials":             "XLF",
    "Health Care":            "XLV",
    "Industrials":            "XLI",
    "Information Technology": "XLK",
    "Materials":              "XLB",
    "Real Estate":            "XLRE",
    "Utilities":              "XLU",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────
# Telegram 알림
# ─────────────────────────────────────────
class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self.token   = token
        self.chat_id = chat_id
        self.enabled = bool(token and chat_id)

    def send(self, message: str) -> bool:
        if not self.enabled:
            log.warning("[Telegram] 미설정 — 알림 건너뜀")
            return False
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {
            "chat_id":    self.chat_id,
            "text":       message,
            "parse_mode": "Markdown",
        }
        try:
            resp = requests.post(url, json=payload, timeout=10)
            resp.raise_for_status()
            return True
        except Exception as e:
            log.error("[Telegram] 전송 실패: %s", e)
            return False


# ─────────────────────────────────────────
# 섹터 ETF 모멘텀 필터
# ─────────────────────────────────────────
class SectorFilter:
    def __init__(
        self,
        sector_map: dict[str, str],         # ticker → GICS sector
        sector_etf_map: dict[str, str],     # GICS sector → ETF ticker
        lookback: int = 60,
        top_n: int = 4,
    ):
        self.sector_map     = sector_map
        self.sector_etf_map = sector_etf_map
        self.lookback       = lookback
        self.top_n          = top_n

    def get_top_sectors(self) -> list[str]:
        """섹터 ETF 60d 모멘텀 상위 top_n 섹터 반환"""
        etfs = list(set(self.sector_etf_map.values()))
        end   = datetime.now(ET)
        start = end - timedelta(days=int(self.lookback * 1.5))
        raw = yf.download(
            etfs,
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
            progress=False, auto_adjust=True,
        )
        closes = raw["Close"] if isinstance(raw.columns, pd.MultiIndex) else raw[["Close"]]
        closes = closes.ffill().dropna(how="all")

        scores: dict[str, float] = {}
        for etf in etfs:
            if etf not in closes.columns:
                continue
            s = closes[etf].dropna()
            if len(s) < self.lookback:
                continue
            scores[etf] = float(s.iloc[-1] / s.iloc[-self.lookback] - 1)

        # ETF 모멘텀 로깅
        etf_to_sector = {v: k for k, v in self.sector_etf_map.items()}
        for etf, ret in sorted(scores.items(), key=lambda x: -x[1]):
            sector = etf_to_sector.get(etf, etf)
            log.info("[Sector] %s (%s): %+.2f%%", etf, sector, ret * 100)

        top_etfs = sorted(scores, key=lambda x: -scores[x])[:self.top_n]
        top_sectors = [etf_to_sector[e] for e in top_etfs if e in etf_to_sector]
        log.info("[Sector] 상위 %d 섹터: %s", self.top_n, top_sectors)
        return top_sectors

    def filter(self, tickers: list[str]) -> list[str]:
        """상위 섹터에 속하는 종목만 반환"""
        top_sectors = self.get_top_sectors()
        filtered = [t for t in tickers if self.sector_map.get(t) in top_sectors]
        log.info("[Sector] 필터 후 유니버스: %d → %d종목", len(tickers), len(filtered))
        return filtered


# ─────────────────────────────────────────
# Momentum-60d 시그널
# ─────────────────────────────────────────
class MomentumStrategy:
    def __init__(
        self,
        universe: list[str],
        lookback: int = 60,
        top_k: int = 5,
        sector_filter: Optional["SectorFilter"] = None,
    ):
        self.universe       = universe
        self.lookback       = lookback
        self.top_k          = top_k
        self.sector_filter  = sector_filter

    def get_scores(self) -> pd.Series:
        """yfinance로 최근 lookback+10일 데이터 조회 후 모멘텀 스코어 계산"""
        # 섹터 필터 적용
        universe = (
            self.sector_filter.filter(self.universe)
            if self.sector_filter else self.universe
        )
        end   = datetime.now(ET)
        start = end - timedelta(days=int(self.lookback * 1.5))  # 공휴일 여유분
        log.info("[Strategy] 가격 데이터 조회 중... %d종목", len(universe))

        raw = yf.download(
            universe,
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
            progress=False,
            auto_adjust=True,
        )
        if isinstance(raw.columns, pd.MultiIndex):
            closes = raw["Close"]
        else:
            closes = raw[["Close"]]

        closes = closes.ffill().dropna(how="all")

        scores: dict[str, float] = {}
        for ticker in universe:
            if ticker not in closes.columns:
                continue
            s = closes[ticker].dropna()
            if len(s) < self.lookback:
                log.warning("[Strategy] %s 데이터 부족 (%d행)", ticker, len(s))
                continue
            ret = s.iloc[-1] / s.iloc[-self.lookback] - 1
            scores[ticker] = float(ret)
            log.info("[Strategy] %s 60d 수익률: %.2f%%", ticker, ret * 100)

        return pd.Series(scores).sort_values(ascending=False)

    def get_target(self) -> list[str]:
        """상위 top_k 종목 반환"""
        scores = self.get_scores()
        selected = scores.head(self.top_k).index.tolist()
        log.info("[Strategy] 선정 종목: %s", selected)
        return selected, scores


# ─────────────────────────────────────────
# Alpaca Paper Trader
# ─────────────────────────────────────────
class AlpacaPaperTrader:
    def __init__(
        self,
        strategy: MomentumStrategy,
        notifier: TelegramNotifier,
    ):
        if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
            raise ValueError("ALPACA_API_KEY / ALPACA_SECRET_KEY 환경변수를 설정하세요.")

        self.client     = TradingClient(
            ALPACA_API_KEY, ALPACA_SECRET_KEY,
            paper=True,
            url_override=ALPACA_BASE_URL or None,
        )
        self.peak_equity: float = 0.0   # 손절 기준 고점 자산
        self.strategy = strategy
        self.notifier = notifier

    # ── 계좌 정보 ──────────────────────────
    def get_equity(self) -> float:
        account = self.client.get_account()
        return float(account.equity)

    def get_positions(self) -> dict[str, float]:
        """현재 보유 포지션 {ticker: qty}"""
        positions = self.client.get_all_positions()
        return {p.symbol: float(p.qty) for p in positions}

    # ── 손절 (고점 대비 -15%) ──────────────
    def check_stop_loss(self) -> bool:
        """고점 대비 -STOP_LOSS_PCT 이상 하락 시 전량 청산 후 True 반환"""
        equity = self.get_equity()
        if self.peak_equity == 0.0:
            self.peak_equity = equity
            return False

        self.peak_equity = max(self.peak_equity, equity)
        drawdown = (equity - self.peak_equity) / self.peak_equity

        if drawdown <= -STOP_LOSS_PCT:
            msg = (
                f"🚨 *손절 발동!*\n"
                f"고점: `${self.peak_equity:,.2f}` → 현재: `${equity:,.2f}`\n"
                f"낙폭: `{drawdown:.2%}` (기준: -{STOP_LOSS_PCT:.0%})\n"
                f"전량 현금화 진행 중..."
            )
            log.warning(msg)
            self.notifier.send(msg)

            positions = self.get_positions()
            for ticker, qty in positions.items():
                self._close_position(ticker, qty)
                self.notifier.send(f"🔴 *손절 청산* `{ticker}` {qty:.0f}주")

            self.peak_equity = 0.0  # 재진입 시 고점 리셋
            self.notifier.send("💵 *전량 현금 보유 — 다음 월요일 재진입 검토*")
            return True

        log.info("[StopLoss] 낙폭=%.2f%% (고점=$%.2f, 현재=$%.2f) — 정상",
                 drawdown * 100, self.peak_equity, equity)
        return False

    # ── 주문 ───────────────────────────────
    def _market_order(self, symbol: str, qty: float, side: OrderSide) -> bool:
        symbol = TICKER_MAP.get(symbol, symbol)  # yfinance → Alpaca 표기 변환
        try:
            req = MarketOrderRequest(
                symbol        = symbol,
                qty           = qty,
                side          = side,
                time_in_force = TimeInForce.DAY,
            )
            order = self.client.submit_order(req)
            log.info("[Order] %s %s %.0f주 (id=%s)", side.value.upper(), symbol, qty, order.id)
            return True
        except Exception as e:
            log.error("[Order] %s %s 실패: %s", side.value.upper(), symbol, e)
            self.notifier.send(f"⚠️ *주문 오류* `{symbol}` {side.value}: `{e}`")
            return False

    def _close_position(self, symbol: str, qty: float) -> bool:
        return self._market_order(symbol, qty, OrderSide.SELL)

    def _open_position(self, symbol: str, qty: float) -> bool:
        return self._market_order(symbol, qty, OrderSide.BUY)

    # ── 현재 가격 ──────────────────────────
    def _get_price(self, symbol: str) -> Optional[float]:
        try:
            data = yf.download(symbol, period="2d", progress=False, auto_adjust=True)
            if data.empty:
                return None
            closes = data["Close"]
            if isinstance(closes, pd.DataFrame):
                closes = closes.iloc[:, 0]
            return float(closes.iloc[-1])
        except Exception as e:
            log.error("[Price] %s 조회 실패: %s", symbol, e)
            return None

    # ── 리밸런싱 핵심 로직 ─────────────────
    def rebalance(self):
        now = datetime.now(ET)
        log.info("=" * 55)
        log.info("리밸런싱 시작: %s", now.strftime("%Y-%m-%d %H:%M ET"))
        log.info("=" * 55)

        self.notifier.send(
            f"📊 *Momentum-60d 리밸런싱 시작*\n"
            f"📅 {now.strftime('%Y-%m-%d %H:%M ET')}"
        )

        # 0. 손절 체크 — 발동 시 현금 보유 후 종료
        if self.check_stop_loss():
            log.warning("손절 발동 — 이번 주 리밸런싱 스킵")
            return

        # 1. 모멘텀 시그널 계산
        try:
            target_tickers, scores = self.strategy.get_target()
        except Exception as e:
            msg = f"❌ 시그널 계산 오류: {e}"
            log.error(msg)
            self.notifier.send(msg)
            return

        score_lines = "\n".join(
            f"  {'✅' if t in target_tickers else '  '} `{t}`: {scores[t]:.2%}"
            for t in scores.index
        )
        self.notifier.send(
            f"📈 *60d 모멘텀 스코어*\n{score_lines}\n\n"
            f"🎯 *선정:* {', '.join(f'`{t}`' for t in target_tickers)}"
        )

        # 2. 현재 포지션 및 계좌 자산 확인
        current_positions = self.get_positions()
        equity = self.get_equity()
        target_per_position = equity / self.strategy.top_k
        log.info("계좌 자산: $%.2f | 목표 포지션당: $%.2f", equity, target_per_position)

        # 3. 청산 (목표 외 종목)
        exits = [t for t in current_positions if t not in target_tickers]
        for ticker in exits:
            qty = current_positions[ticker]
            log.info("[청산] %s %.0f주", ticker, qty)
            if self._close_position(ticker, qty):
                self.notifier.send(f"🔴 *청산* `{ticker}` {qty:.0f}주")

        # 잔량 정산 대기
        if exits:
            log.info("청산 주문 체결 대기 (5초)...")
            time.sleep(5)
            equity = self.get_equity()
            target_per_position = equity / self.strategy.top_k

        # 4. 매수 (목표 종목)
        current_positions = self.get_positions()
        entries: list[tuple[str, float]] = []

        for ticker in target_tickers:
            price = self._get_price(ticker)
            if price is None:
                log.warning("[매수 스킵] %s 가격 조회 실패", ticker)
                continue

            target_qty = int(target_per_position / price)
            current_qty = current_positions.get(ticker, 0.0)
            delta_qty = target_qty - int(current_qty)

            if delta_qty > 0:
                log.info("[매수] %s %d주 @ $%.2f", ticker, delta_qty, price)
                if self._open_position(ticker, delta_qty):
                    entries.append((ticker, delta_qty))
                    self.notifier.send(
                        f"🟢 *매수* `{ticker}` {delta_qty}주 @ ${price:.2f}"
                    )
            elif delta_qty < 0:
                # 비중 초과 → 일부 청산
                trim_qty = abs(delta_qty)
                log.info("[비중조정] %s -%d주", ticker, trim_qty)
                if self._close_position(ticker, trim_qty):
                    self.notifier.send(
                        f"🟡 *비중 조정* `{ticker}` -{trim_qty}주"
                    )
            else:
                log.info("[유지] %s (변동 없음)", ticker)

        # 5. 완료 리포트
        final_equity = self.get_equity()
        final_positions = self.get_positions()

        pos_lines = "\n".join(
            f"  `{t}`: {int(q)}주"
            for t, q in final_positions.items()
        ) or "  (없음)"

        summary = (
            f"✅ *리밸런싱 완료*\n"
            f"💰 계좌 자산: `${final_equity:,.2f}`\n"
            f"📋 *보유 포지션:*\n{pos_lines}\n"
            f"🕐 {datetime.now(ET).strftime('%H:%M ET')}"
        )
        log.info(summary)
        self.notifier.send(summary)


# ─────────────────────────────────────────
# 스케줄러 (매주 월요일 09:35 ET)
# ─────────────────────────────────────────
def _is_market_open(trader: AlpacaPaperTrader) -> bool:
    try:
        clock = trader.client.get_clock()
        return clock.is_open
    except Exception as e:
        log.warning("시장 상태 확인 실패: %s", e)
        return False


def _is_first_monday() -> bool:
    """오늘이 이번 달 첫째 월요일인지 확인"""
    now = datetime.now(ET)
    if now.weekday() != 0:
        return False
    # 같은 달에 오늘보다 이른 월요일이 없으면 첫째 월요일
    return now.day <= 7


def run_if_first_monday(trader: AlpacaPaperTrader):
    now = datetime.now(ET)
    if not _is_first_monday():
        log.info("첫째 월요일 아님 (%s) — 스킵", now.strftime("%Y-%m-%d"))
        return
    if not _is_market_open(trader):
        log.warning("시장 휴장 — 리밸런싱 스킵")
        trader.notifier.send("⚠️ 시장 휴장 — 이번 달 첫째 월요일 리밸런싱 스킵")
        return
    trader.rebalance()


def _daily_stop_loss_check(trader: AlpacaPaperTrader):
    """장 마감 후 일별 손절 체크 (매일 16:05 ET)"""
    now = datetime.now(ET)
    if now.weekday() >= 5:  # 주말 스킵
        return
    if not _is_market_open(trader):
        return
    log.info("[일별 손절 체크] %s", now.strftime("%Y-%m-%d"))
    trader.check_stop_loss()


def run_scheduler(trader: AlpacaPaperTrader):
    log.info("스케줄러 시작 — 매월 첫째 월요일 %s ET 리밸런싱 | 손절 -%.0f%%",
             REBAL_TIME, STOP_LOSS_PCT * 100)
    trader.notifier.send(
        f"🤖 *Alpaca Paper Trader 시작*\n"
        f"전략: Momentum-60d | Top-{trader.strategy.top_k} | 매월 첫째 월요일 {REBAL_TIME} ET\n"
        f"손절: 고점 대비 -{STOP_LOSS_PCT:.0%} 발동"
    )

    schedule.every().day.at(REBAL_TIME).do(run_if_first_monday, trader=trader)
    schedule.every().day.at("16:05").do(_daily_stop_loss_check, trader=trader)

    while True:
        schedule.run_pending()
        time.sleep(30)


# ─────────────────────────────────────────
# 백테스트: 섹터 필터 유무 비교
# ─────────────────────────────────────────
def run_backtest(
    universe: list[str],
    sector_map: dict[str, str],
    start: str = "2020-01-01",
    end: str = "2024-12-31",
    lookback: int = 60,
    top_k: int = 10,
    top_sectors: int = 4,
) -> None:
    """섹터 ETF 필터 유무 백테스트 비교 (주간 리밸런싱, 슬리피지 포함)"""
    COST = 0.001  # 편도 0.1% 슬리피지+수수료

    all_tickers = list(set(universe + list(SECTOR_ETF_MAP.values())))
    log.info("가격 데이터 다운로드 중... (%d tickers)", len(all_tickers))
    raw = yf.download(all_tickers, start=start, end=end, progress=False, auto_adjust=True)
    closes = (raw["Close"] if isinstance(raw.columns, pd.MultiIndex) else raw).ffill()
    log.info("다운로드 완료: %d일 x %d종목", len(closes), closes.shape[1])

    etf_tickers = list(set(SECTOR_ETF_MAP.values()))
    etf_to_sector = {v: k for k, v in SECTOR_ETF_MAP.items()}

    all_dates = closes.index
    # 주간 리밸런싱: 매주 월요일(혹은 실제 거래일)
    dow = pd.Series(all_dates.dayofweek, index=all_dates)
    weekly = all_dates[
        (dow == 0) |  # 월요일
        ((dow < 4) & (dow.shift(1, fill_value=4) >= 4))  # 월요일 휴장 시 화요일 대체
    ]
    weekly = weekly.drop_duplicates()

    equity_base: list[float] = [1.0]
    equity_sect: list[float] = [1.0]
    prev_base: list[str] = []
    prev_sect: list[str] = []
    traded_weeks = 0

    for i in range(len(weekly) - 1):
        date = weekly[i]
        next_date = weekly[i + 1]

        pos = all_dates.searchsorted(date)
        next_pos = all_dates.searchsorted(next_date)
        if pos < lookback or next_pos >= len(closes):
            continue

        window = closes.iloc[pos - lookback: pos + 1]

        # ── 종목 모멘텀 ──
        stock_rets: dict[str, float] = {}
        for t in universe:
            if t not in window.columns:
                continue
            s = window[t].dropna()
            if len(s) < lookback * 0.8 or s.iloc[0] <= 0:
                continue
            stock_rets[t] = float(s.iloc[-1] / s.iloc[0] - 1)

        if not stock_rets:
            continue

        # ── Baseline: 전체 유니버스 top-k ──
        base_picks = sorted(stock_rets, key=lambda x: -stock_rets[x])[:top_k]

        # ── Sector: 섹터 ETF 필터 후 top-k ──
        etf_rets: dict[str, float] = {}
        for e in etf_tickers:
            if e not in window.columns:
                continue
            s = window[e].dropna()
            if len(s) >= lookback * 0.8 and s.iloc[0] > 0:
                etf_rets[e] = float(s.iloc[-1] / s.iloc[0] - 1)

        top_etfs = sorted(etf_rets, key=lambda x: -etf_rets[x])[:top_sectors]
        top_secs = {etf_to_sector[e] for e in top_etfs if e in etf_to_sector}
        filtered = {t: r for t, r in stock_rets.items() if sector_map.get(t) in top_secs}
        sect_picks = sorted(filtered, key=lambda x: -filtered[x])[:top_k] if filtered else base_picks

        # ── 기간 수익률 계산 (슬리피지 포함) ──
        def period_ret(picks: list[str], prev: list[str]) -> float:
            rets = []
            for t in picks:
                if t not in closes.columns:
                    continue
                p0 = closes[t].iloc[pos]
                p1 = closes[t].iloc[next_pos]
                if pd.isna(p0) or pd.isna(p1) or p0 <= 0:
                    continue
                rets.append(float(p1 / p0 - 1))
            if not rets:
                return 0.0
            turnover = len(set(picks) - set(prev)) / max(len(picks), 1)
            return float(np.mean(rets)) - turnover * COST * 2

        equity_base.append(equity_base[-1] * (1 + period_ret(base_picks, prev_base)))
        equity_sect.append(equity_sect[-1] * (1 + period_ret(sect_picks, prev_sect)))
        prev_base = base_picks
        prev_sect = sect_picks
        traded_weeks += 1

    # ── 성과 지표 출력 ──
    def metrics(equity: list[float], label: str) -> None:
        eq = pd.Series(equity)
        total = float(eq.iloc[-1] - 1)
        n_years = traded_weeks / 52
        cagr = float(eq.iloc[-1] ** (1 / n_years) - 1) if n_years > 0 else 0.0
        wr = eq.pct_change().dropna()
        sharpe = float(wr.mean() / wr.std() * np.sqrt(52)) if wr.std() > 0 else 0.0
        mdd = float(((eq / eq.cummax()) - 1).min())
        print(f"\n{'='*56}")
        print(f"  {label}")
        print(f"{'='*56}")
        print(f"  total_return : {total:+.2%}")
        print(f"  cagr         : {cagr:+.2%}")
        print(f"  mdd          : {mdd:.2%}")
        print(f"  sharpe       : {sharpe:.2f}")
        print(f"{'='*56}")

    print(f"\n백테스트 기간: {start} ~ {end} | {traded_weeks}주 리밸런싱")
    print(f"파라미터: lookback={lookback}d | top_k={top_k} | top_sectors={top_sectors}")
    metrics(equity_base, f"Baseline - 섹터 필터 없음 | top-{top_k}")
    metrics(equity_sect, f"Sector Filter - 상위 {top_sectors}섹터 내 top-{top_k}")


# ─────────────────────────────────────────
# CLI
# ─────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Alpaca Paper Trader — Momentum-60d")
    parser.add_argument(
        "--run-now", action="store_true",
        help="스케줄 무시하고 지금 즉시 리밸런싱 실행"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="시그널만 계산하고 주문 없이 결과 출력"
    )
    parser.add_argument(
        "--top-k", type=int, default=TOP_K,
        help=f"상위 종목 수 (기본: {TOP_K})"
    )
    parser.add_argument(
        "--top-sectors", type=int, default=TOP_SECTORS,
        help=f"상위 섹터 수 (기본: {TOP_SECTORS}, 0=비활성)"
    )
    parser.add_argument(
        "--backtest", action="store_true",
        help="섹터 필터 유무 백테스트 비교 실행"
    )
    parser.add_argument("--start", default="2020-01-01", help="백테스트 시작일 (기본: 2020-01-01)")
    parser.add_argument("--end",   default="2024-12-31", help="백테스트 종료일 (기본: 2024-12-31)")
    args = parser.parse_args()

    notifier = TelegramNotifier(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)

    log.info("S&P500 종목 리스트 로딩 중...")
    universe, sector_map = get_sp500_with_sectors()
    log.info("유니버스: %d종목", len(universe))

    sector_filter = None
    if args.top_sectors > 0:
        sector_filter = SectorFilter(
            sector_map=sector_map,
            sector_etf_map=SECTOR_ETF_MAP,
            lookback=LOOKBACK,
            top_n=args.top_sectors,
        )
        log.info("섹터 ETF 모멘텀 필터 활성 (상위 %d섹터)", args.top_sectors)

    strategy = MomentumStrategy(universe, lookback=LOOKBACK, top_k=args.top_k,
                                sector_filter=sector_filter)

    if args.backtest:
        run_backtest(
            universe=universe,
            sector_map=sector_map,
            start=args.start,
            end=args.end,
            lookback=LOOKBACK,
            top_k=args.top_k,
            top_sectors=args.top_sectors,
        )
    elif args.dry_run:
        print("\n=== Dry Run: 시그널 계산만 ===")
        target, scores = strategy.get_target()
        print(f"\n60d 모멘텀 스코어:")
        for t, v in scores.items():
            mark = "★" if t in target else " "
            print(f"  {mark} {t:<8}: {v:+.2%}")
        print(f"\n선정 종목 (top {args.top_k}): {target}")
    else:
        trader = AlpacaPaperTrader(strategy, notifier)
        if args.run_now:
            trader.rebalance()
        else:
            run_scheduler(trader)
