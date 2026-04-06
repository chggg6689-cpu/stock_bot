"""
signals/orb.py
ORBSignal — 진입·청산 시그널 판단 (순수 로직, 부수효과 없음)

진입 조건:
  1. 현재 바 종가 > OR 고점 × (1 + ORB_BUFFER)
  2. 현재 바 거래량 > OR 평균 거래량 × VOL_SURGE_MULT
  3. 현재 시각 < ENTRY_DEADLINE

청산 조건 (우선순위 순):
  1. TP  : 고가 >= 진입가 × (1 + TAKE_PROFIT)
  2. SL  : 저가 <= max(OR 저점, 진입가 × (1 - STOP_LOSS_PCT))
  3. EOD : 현재 시각 >= FORCE_CLOSE
"""
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

import pandas as pd

from config import (
    ORB_BUFFER,
    VOL_SURGE_MULT,
    TAKE_PROFIT,
    STOP_LOSS_PCT,
    ENTRY_DEADLINE,
    FORCE_CLOSE,
    ET,
)

log = logging.getLogger(__name__)

ExitReason = Literal["TP", "SL", "EOD", None]


@dataclass
class Position:
    """진입 후 포지션 상태"""
    ticker:      str
    entry_price: float          # 실제 체결가 (슬리피지 반영)
    entry_ref:   float          # 돌파 기준가 (OR 고점 + 버퍼)
    entry_time:  datetime
    or_high:     float
    or_low:      float
    shares:      int = 0
    tp_price:    float = field(init=False)
    sl_price:    float = field(init=False)

    def __post_init__(self):
        self.tp_price = self.entry_price * (1 + TAKE_PROFIT)
        self.sl_price = max(self.or_low, self.entry_price * (1 - STOP_LOSS_PCT))


class ORBSignal:
    """
    ORB 전략의 진입·청산 시그널을 판단한다.
    DataManager / ExecutionSimulator와 분리된 순수 로직 레이어.
    """

    # ─────────────────────────────────────────
    # 진입 시그널
    # ─────────────────────────────────────────
    def check_entry(
        self,
        or_features: dict,
        bar: pd.Series,
        ts: datetime,
    ) -> bool:
        """
        단일 1분봉 바에서 진입 조건 충족 여부 반환.

        Args:
            or_features : FeatureBuilder.build_or_features() 반환값
            bar         : 현재 1분봉 행 (High/Low/Close/Volume 포함)
            ts          : 현재 바 타임스탬프 (ET aware)

        Returns:
            True  → 진입 조건 충족
            False → 미충족
        """
        if not or_features.get("valid", False):
            return False

        # 진입 마감 시간 초과
        if not self._within_entry_window(ts):
            return False

        or_high    = or_features["or_high"]
        or_vol_avg = or_features["or_vol_avg"]

        price  = float(bar.get("Close", 0))
        volume = float(bar.get("Volume", 0))

        threshold = or_high * (1 + ORB_BUFFER)
        price_ok  = price > threshold
        vol_ok    = volume > or_vol_avg * VOL_SURGE_MULT

        if price_ok and vol_ok:
            log.debug(
                "진입 시그널: price=%.2f > thr=%.2f  vol=%.0f > avg×%.1f=%.0f",
                price, threshold, volume, VOL_SURGE_MULT, or_vol_avg * VOL_SURGE_MULT,
            )
        return price_ok and vol_ok

    def entry_ref_price(self, or_features: dict) -> float:
        """진입 기준가 (OR 고점 × (1 + ORB_BUFFER))"""
        return or_features["or_high"] * (1 + ORB_BUFFER)

    # ─────────────────────────────────────────
    # 청산 시그널
    # ─────────────────────────────────────────
    def check_exit(
        self,
        position: Position,
        bar: pd.Series,
        ts: datetime,
    ) -> ExitReason:
        """
        단일 1분봉 바에서 청산 조건 확인.

        Returns:
            "TP"  → 익절 조건 충족 (고가 >= TP)
            "SL"  → 손절 조건 충족 (저가 <= SL)
            "EOD" → 장마감 강제 청산
            None  → 청산 없음
        """
        # EOD 우선 (시간 초과 시 즉시 청산)
        if self._is_eod(ts):
            return "EOD"

        hi = float(bar.get("High", 0))
        lo = float(bar.get("Low",  0))

        if hi >= position.tp_price:
            log.debug("TP: high=%.2f >= tp=%.2f", hi, position.tp_price)
            return "TP"

        if lo <= position.sl_price:
            log.debug("SL: low=%.2f <= sl=%.2f", lo, position.sl_price)
            return "SL"

        return None

    def exit_ref_price(self, reason: ExitReason, position: Position, bar: pd.Series) -> float:
        """
        청산 이유별 기준가 반환.
          TP  → position.tp_price
          SL  → position.sl_price
          EOD → 현재 바 종가
        """
        if reason == "TP":
            return position.tp_price
        if reason == "SL":
            return position.sl_price
        return float(bar.get("Close", 0))

    # ─────────────────────────────────────────
    # 내부 유틸
    # ─────────────────────────────────────────
    @staticmethod
    def _within_entry_window(ts: datetime) -> bool:
        """09:35 ~ ENTRY_DEADLINE 사이인지 확인"""
        h, m = ts.hour, ts.minute
        after_open = (h > 9) or (h == 9 and m >= 35)
        before_deadline = (h < ENTRY_DEADLINE[0]) or (
            h == ENTRY_DEADLINE[0] and m < ENTRY_DEADLINE[1]
        )
        return after_open and before_deadline

    @staticmethod
    def _is_eod(ts: datetime) -> bool:
        """FORCE_CLOSE 시각 이상인지 확인"""
        h, m = ts.hour, ts.minute
        return h > FORCE_CLOSE[0] or (h == FORCE_CLOSE[0] and m >= FORCE_CLOSE[1])
