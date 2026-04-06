"""
signals/filters.py
보조 시그널 필터 — ORBSignal과 독립적으로 동작하는 순수 함수 레이어

1. RegimeFilter  : QQQ 오픈 세션 모멘텀 기반 레짐 필터
2. RSMomentumFilter : 장중 종목 RS > QQQ 조건 필터

Lookahead bias 방지 원칙:
  - 모든 함수는 ts(판단 시점) 이전 데이터만 사용
  - OR 피처는 09:34 바 종료 후에만 확정
  - 장중 RS/VWAP는 ts 이전 bars만 슬라이싱
"""
import logging
from datetime import datetime

import pandas as pd

log = logging.getLogger(__name__)


class RegimeFilter:
    """
    QQQ 오픈 세션(09:30~09:35) 모멘텀으로 당일 레짐을 판단한다.
    OR 구간(09:34 완료) 이후에만 호출 가능 → lookahead bias 없음.

    판단 기준:
      - QQQ OR 범위 > threshold  AND  QQQ OR 방향 상승 → "bull"
      - QQQ OR 범위 > threshold  AND  QQQ OR 방향 하락 → "bear"
      - 그 외 → "neutral"
    """

    def __init__(self, or_range_threshold: float = 0.003):
        """
        Args:
            or_range_threshold: QQQ OR 범위 최소값 (기본 0.3%)
                                이 미만이면 방향성 없음 → "neutral"
        """
        self.threshold = or_range_threshold

    def regime(self, qqq_or_features: dict) -> str:
        """
        QQQ OR 피처로 레짐 반환.

        Args:
            qqq_or_features: FeatureBuilder.build_or_features(qqq_bars) 반환값

        Returns:
            "bull" / "bear" / "neutral"

        Lookahead: OR 피처는 09:34 완료 후 확정 → 09:35 진입 판단에 사용 안전
        """
        if not qqq_or_features.get("valid", False):
            return "neutral"

        or_range = qqq_or_features["or_range"]
        or_open  = qqq_or_features["or_open"]
        or_high  = qqq_or_features["or_high"]
        or_low   = qqq_or_features["or_low"]

        if or_range < self.threshold:
            return "neutral"

        # OR 중간값 기준으로 방향 판단
        or_mid   = (or_high + or_low) / 2
        or_close = qqq_or_features.get("or_close", or_mid)  # 있으면 사용

        direction = "bull" if or_close >= or_mid else "bear"
        log.debug(
            "RegimeFilter: range=%.3f%% dir=%s open=%.2f high=%.2f low=%.2f",
            or_range * 100, direction, or_open, or_high, or_low,
        )
        return direction

    def allows_long(self, qqq_or_features: dict) -> bool:
        """롱 진입 허용 여부 (bull 또는 neutral만 허용)"""
        return self.regime(qqq_or_features) in ("bull", "neutral")


class RSMomentumFilter:
    """
    장중 종목 상대강도(RS) 모멘텀 필터.
    종목 RS > QQQ 기준(0) 이어야 진입 허용.

    Lookahead bias 방지:
      - intraday_rs() 는 ts 이전 bars만 사용
      - 진입 시그널 판단 시점(ts)과 동일한 bars_to_ts 슬라이싱 사용
    """

    def __init__(self, min_rs: float = 0.0):
        """
        Args:
            min_rs: 최소 RS 임계값 (기본 0 = QQQ 대비 양수)
        """
        self.min_rs = min_rs

    def allows_entry(self, rs_features: dict) -> bool:
        """
        RS 기반 진입 허용 여부.

        Args:
            rs_features: FeatureBuilder.intraday_rs() 반환값
                         (rs_vs_qqq, ticker_chg, qqq_chg, is_leader)

        Returns:
            True → RS >= min_rs (종목이 QQQ 대비 강함)
            False → 후행
        """
        rs = rs_features.get("rs_vs_qqq", 0.0)
        ok = rs >= self.min_rs
        log.debug("RSMomentumFilter: rs=%.4f min=%.4f → %s", rs, self.min_rs, ok)
        return ok

    def score(self, rs_features: dict) -> float:
        """진입 강도 점수 (높을수록 강한 RS)"""
        return rs_features.get("rs_vs_qqq", 0.0)


class CompositeFilter:
    """
    RegimeFilter + RSMomentumFilter 조합.
    두 필터 모두 통과해야 진입 허용.
    """

    def __init__(
        self,
        regime_filter: RegimeFilter | None = None,
        rs_filter: RSMomentumFilter | None = None,
    ):
        self.regime_filter = regime_filter or RegimeFilter()
        self.rs_filter     = rs_filter     or RSMomentumFilter()

    def allows_entry(
        self,
        qqq_or_features: dict,
        rs_features: dict,
    ) -> tuple[bool, str]:
        """
        복합 필터 진입 허용 여부.

        Returns:
            (허용 여부: bool, 거부 이유: str)
            허용 시 이유는 빈 문자열
        """
        if not self.regime_filter.allows_long(qqq_or_features):
            regime = self.regime_filter.regime(qqq_or_features)
            return False, f"레짐 필터 거부: {regime}"

        if not self.rs_filter.allows_entry(rs_features):
            rs = rs_features.get("rs_vs_qqq", 0.0)
            return False, f"RS 필터 거부: rs={rs:.4f}"

        return True, ""
