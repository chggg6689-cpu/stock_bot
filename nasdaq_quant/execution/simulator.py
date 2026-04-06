"""
execution/simulator.py
ExecutionSimulator — 슬리피지, 스프레드, 수수료 모델

슬리피지 공식:
  fill = ref × (1 + dir × (spread + impact))
  spread = SPREAD_BPS[tier]      # half-spread
  impact = MARKET_IMPACT_COEF × (order_value / avg_daily_value)
  dir = +1 (매수), -1 (매도)
"""
import logging
from dataclasses import dataclass

from config import (
    SPREAD_BPS,
    MARKET_IMPACT_COEF,
    COMMISSION_PER_SHARE,
    COMMISSION_MIN,
    get_spread_tier,
)

log = logging.getLogger(__name__)


@dataclass
class FillResult:
    """단일 체결 결과"""
    ref_price:   float   # 기준가 (TP/SL/돌파가 등)
    fill_price:  float   # 실제 체결가 (슬리피지 반영)
    spread_cost: float   # 스프레드 비용 ($)
    impact_cost: float   # 시장충격 비용 ($)
    commission:  float   # 수수료 ($)
    total_cost:  float   # spread + impact + commission ($)
    slip_pct:    float   # 슬리피지 비율 (fill/ref - 1)


class ExecutionSimulator:
    """
    Alpaca 기준 체결 비용 시뮬레이터.

    - 스프레드: 종목 티어별 half-spread 적용
    - 시장충격: 주문금액 / 일거래대금 비례
    - 수수료: max($1.00, shares × $0.005)
    """

    def fill_price(
        self,
        ticker: str,
        side: str,          # "buy" or "sell"
        ref_price: float,
        shares: int,
        avg_daily_volume: float,
    ) -> FillResult:
        """
        슬리피지 반영 체결가 계산.

        Args:
            ticker           : 종목 티커
            side             : "buy" or "sell"
            ref_price        : 기준가 (돌파가, TP가, SL가 등)
            shares           : 주문 수량
            avg_daily_volume : 일평균 거래량 (주)

        Returns:
            FillResult (fill_price, 비용 명세 포함)
        """
        if ref_price <= 0 or shares <= 0:
            return FillResult(ref_price, ref_price, 0.0, 0.0, 0.0, 0.0, 0.0)

        tier    = get_spread_tier(ticker)
        spread  = SPREAD_BPS[tier]             # half-spread 비율

        # 시장충격: 주문금액 / 일거래대금
        order_value     = shares * ref_price
        avg_daily_value = max(avg_daily_volume * ref_price, 1.0)
        impact          = MARKET_IMPACT_COEF * (order_value / avg_daily_value)

        total_slip = spread + impact
        direction  = 1 if side.lower() == "buy" else -1
        fill       = ref_price * (1 + direction * total_slip)

        spread_cost = shares * ref_price * spread
        impact_cost = shares * ref_price * impact
        comm        = self.commission(shares, ref_price)
        total_cost  = spread_cost + impact_cost + comm
        slip_pct    = (fill - ref_price) / ref_price

        log.debug(
            "%s %s %d주 @%.2f → fill=%.4f  tier=%s slip=%.4f%%",
            side.upper(), ticker, shares, ref_price, fill, tier, slip_pct * 100,
        )

        return FillResult(
            ref_price=ref_price,
            fill_price=fill,
            spread_cost=spread_cost,
            impact_cost=impact_cost,
            commission=comm,
            total_cost=total_cost,
            slip_pct=slip_pct,
        )

    def commission(self, shares: int, price: float) -> float:
        """Alpaca 수수료: max($1.00, shares × $0.005)"""
        return max(COMMISSION_MIN, shares * COMMISSION_PER_SHARE)

    def round_trip_cost(
        self,
        ticker: str,
        shares: int,
        entry_ref: float,
        exit_ref: float,
        avg_daily_volume: float,
    ) -> dict:
        """
        왕복(매수+매도) 체결 비용 합계.

        Returns:
            dict with keys:
              entry_fill, exit_fill,
              total_slip_cost ($), total_commission ($), total_cost ($),
              net_pnl_adj: exit_fill - entry_fill (순 손익 단가 차이)
        """
        buy  = self.fill_price(ticker, "buy",  entry_ref, shares, avg_daily_volume)
        sell = self.fill_price(ticker, "sell", exit_ref,  shares, avg_daily_volume)

        total_slip_cost  = buy.spread_cost + buy.impact_cost + sell.spread_cost + sell.impact_cost
        total_commission = buy.commission  + sell.commission
        total_cost       = buy.total_cost  + sell.total_cost

        gross_pnl = (exit_ref   - entry_ref)   * shares
        net_pnl   = (sell.fill_price - buy.fill_price) * shares - total_commission

        return {
            "entry_fill":       buy.fill_price,
            "exit_fill":        sell.fill_price,
            "total_slip_cost":  total_slip_cost,
            "total_commission": total_commission,
            "total_cost":       total_cost,
            "gross_pnl":        gross_pnl,
            "net_pnl":          net_pnl,
            "cost_drag_pct":    total_cost / (entry_ref * shares) if entry_ref > 0 else 0.0,
        }
