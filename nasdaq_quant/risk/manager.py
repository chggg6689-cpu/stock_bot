"""
risk/manager.py
RiskManager — 포지션 사이징, 일일 손실 한도, 유니버스 필터

규칙:
  - 종목당 자본 = capital / n_positions (동일비중)
  - 하루 실현 손익이 -MAX_DAILY_LOSS × capital 초과 시 당일 매매 중단
  - 최소 주가 MIN_PRICE, 최소 일평균 거래량 MIN_AVG_VOLUME 필터
"""
import logging
import math
from dataclasses import dataclass, field

import pandas as pd

from config import (
    CAPITAL,
    MAX_DAILY_LOSS,
    MIN_PRICE,
    MIN_AVG_VOLUME,
    STOP_LOSS_PCT,
    TOP_N,
)

log = logging.getLogger(__name__)


@dataclass
class DailyRiskState:
    """
    하루 리스크 상태 추적.
    Backtester가 거래일마다 초기화해서 사용한다.
    """
    capital:         float
    max_positions:   int   = TOP_N
    realized_pnl:    float = 0.0
    trade_count:     int   = 0
    open_positions:  set   = field(default_factory=set)   # 현재 보유 종목
    blocked:         bool  = False   # 일일 한도 초과 시 True
    block_reason:    str   = ""

    @property
    def loss_limit(self) -> float:
        """당일 허용 최대 손실액 (음수)"""
        return -abs(self.capital * MAX_DAILY_LOSS)

    @property
    def n_open(self) -> int:
        """현재 보유 종목 수"""
        return len(self.open_positions)

    def open_position(self, ticker: str) -> None:
        """포지션 진입 등록"""
        self.open_positions.add(ticker)

    def close_position(self, ticker: str, pnl_dollar: float) -> None:
        """포지션 청산 등록 및 손익 반영"""
        self.open_positions.discard(ticker)
        self.realized_pnl += pnl_dollar
        self.trade_count  += 1
        if self.realized_pnl <= self.loss_limit and not self.blocked:
            self.blocked      = True
            self.block_reason = (
                f"일일 손실 한도 초과: {self.realized_pnl:+.2f} <= {self.loss_limit:.2f}"
            )
            log.warning("당일 매매 중단: %s", self.block_reason)

    def record_trade(self, pnl_dollar: float) -> None:
        """청산 없이 손익만 반영 (하위 호환)"""
        self.close_position("_legacy_", pnl_dollar)
        self.open_positions.discard("_legacy_")


class RiskManager:
    """
    포지션 사이징, 일일 리스크 한도, 유니버스 필터를 담당한다.
    ExecutionSimulator / ORBSignal과 독립적인 순수 로직 레이어.
    """

    def __init__(
        self,
        capital: float = CAPITAL,
        max_daily_loss: float = MAX_DAILY_LOSS,
        min_price: float = MIN_PRICE,
        min_avg_volume: float = MIN_AVG_VOLUME,
        max_positions: int = TOP_N,
        stop_loss_pct: float = STOP_LOSS_PCT,
    ):
        self.capital        = capital
        self.max_daily_loss = max_daily_loss
        self.min_price      = min_price
        self.min_avg_volume = min_avg_volume
        self.max_positions  = max_positions
        self.stop_loss_pct  = stop_loss_pct

    # ─────────────────────────────────────────
    # 포지션 사이징
    # ─────────────────────────────────────────
    def position_size(
        self,
        price: float,
        n_positions: int = TOP_N,
        capital: float | None = None,
    ) -> int:
        """
        동일비중 기준 종목당 투자금으로 매수 가능한 주수 반환.

        Args:
            price       : 현재 주가 (진입 체결가)
            n_positions : 동시 보유 종목 수 (기본 TOP_N)
            capital     : 총 자본 (None이면 self.capital 사용)

        Returns:
            정수 주수 (0이면 진입 불가 — 자본 부족 또는 가격 이상)
        """
        cap = capital if capital is not None else self.capital
        if price <= 0 or n_positions <= 0:
            return 0
        alloc   = cap / n_positions          # 종목당 투자금
        shares  = math.floor(alloc / price)  # 소수점 내림
        return max(0, shares)

    def position_value(
        self,
        price: float,
        n_positions: int = TOP_N,
        capital: float | None = None,
    ) -> float:
        """종목당 투자금액 (달러)"""
        cap    = capital if capital is not None else self.capital
        shares = self.position_size(price, n_positions, cap)
        return shares * price

    # ─────────────────────────────────────────
    # 일일 손실 한도
    # ─────────────────────────────────────────
    def new_daily_state(self, capital: float | None = None) -> DailyRiskState:
        """새 거래일 DailyRiskState 생성"""
        return DailyRiskState(
            capital=capital or self.capital,
            max_positions=self.max_positions,
        )

    def daily_loss_ok(self, state: DailyRiskState) -> bool:
        """
        당일 추가 매매 허용 여부.

        Returns:
            True  → 매매 계속 가능
            False → 일일 손실 한도 초과, 당일 신규 진입 중단
        """
        return not state.blocked

    # ─────────────────────────────────────────
    # 유니버스 필터
    # ─────────────────────────────────────────
    def filter_universe(
        self,
        candidates: list[str],
        prev_ohlcv: pd.DataFrame,
    ) -> list[str]:
        """
        가격·유동성 기준으로 후보 종목을 필터링한다.

        Args:
            candidates  : 스코어 상위 종목 리스트
            prev_ohlcv  : 전일 OHLCV DataFrame (index=ticker,
                          columns=[high, low, close, volume])

        Returns:
            필터 통과 종목 리스트 (입력 순서 유지)
        """
        if prev_ohlcv.empty:
            return []

        passed = []
        for ticker in candidates:
            if ticker not in prev_ohlcv.index:
                log.debug("%s: 전일 OHLCV 없음 → 제외", ticker)
                continue
            row    = prev_ohlcv.loc[ticker]
            close  = float(row.get("close",  row.get("Close",  0)))
            volume = float(row.get("volume", row.get("Volume", 0)))

            if close < self.min_price:
                log.debug("%s: 주가 $%.2f < $%.2f → 제외", ticker, close, self.min_price)
                continue
            if volume < self.min_avg_volume:
                log.debug("%s: 거래량 %.0f < %.0f → 제외",
                          ticker, volume, self.min_avg_volume)
                continue
            passed.append(ticker)

        log.info("유니버스 필터: %d → %d종목", len(candidates), len(passed))
        return passed

    def max_loss_per_position(self, price: float, shares: int) -> float:
        """
        종목당 최대 손실액 (달러, 양수).
        SL 기준: entry_price × stop_loss_pct × shares
        """
        return price * self.stop_loss_pct * shares

    def regime_allows_entry(self, regime_features: dict) -> tuple[bool, str]:
        """
        레짐이 약할 때 진입 제한. signals 레이어의 RegimeFilter 출력값을 사용.

        Args:
            regime_features: RegimeFilter 또는 FeatureBuilder.qqq_regime_features() 반환값
                             {"qqq_regime": "bull"/"bear"/"neutral", ...}

        Returns:
            (허용: bool, 거부 이유: str)
            bear 레짐에서는 롱 진입 금지.
        """
        regime = regime_features.get("qqq_regime", "neutral")
        if regime == "bear":
            return False, f"레짐 bear → 롱 진입 금지"
        return True, ""

    def validate_entry(
        self,
        ticker: str,
        price: float,
        shares: int,
        state: DailyRiskState,
        regime_features: dict | None = None,
    ) -> tuple[bool, str]:
        """
        단일 진입 직전 최종 리스크 검사 (순서대로 체크).

        검사 항목:
          1. 일일 손실 한도
          2. 동시 보유 종목 수 한도
          3. 최소 주가
          4. 주수 > 0
          5. 레짐 필터 (regime_features 제공 시)

        Returns:
            (허용: bool, 거부 이유: str)
        """
        if not self.daily_loss_ok(state):
            return False, f"일일 손실 한도: {state.block_reason}"

        if state.n_open >= self.max_positions:
            return False, f"동시 보유 한도 {self.max_positions}종목 초과 (현재 {state.n_open})"

        if price < self.min_price:
            return False, f"{ticker} 주가 ${price:.2f} < 최소 ${self.min_price}"

        if shares <= 0:
            return False, f"{ticker} 주수=0 (자본 부족)"

        if regime_features is not None:
            ok, reason = self.regime_allows_entry(regime_features)
            if not ok:
                return False, reason

        return True, ""
