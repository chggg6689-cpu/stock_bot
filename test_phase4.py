"""Phase 4 risk 레이어 테스트"""
import pandas as pd
from nasdaq_quant.risk import RiskManager, DailyRiskState

rm = RiskManager(capital=100_000, max_positions=5)

# ═══════════════════════════════════════════════════════════════
# T-R1: 포지션 사이징
# ═══════════════════════════════════════════════════════════════
shares = rm.position_size(price=50.0, n_positions=5)
assert shares == 400, f"기대 400주, 실제 {shares}"   # 100k/5=20k, 20k/50=400
print(f"[OK] position_size: $50 / 5포지션 = {shares}주")

shares_high = rm.position_size(price=800.0, n_positions=5)
assert shares_high == 25                               # 20k/800=25
print(f"[OK] position_size: $800 / 5포지션 = {shares_high}주")

shares_zero = rm.position_size(price=0.0, n_positions=5)
assert shares_zero == 0
print("[OK] position_size: price=0 → 0주")

# ═══════════════════════════════════════════════════════════════
# T-R2: 종목당 최대 손실 (달러)
# ═══════════════════════════════════════════════════════════════
# entry=$50, shares=400, SL=3.5% → max loss = 50×0.035×400 = $700
max_loss = rm.max_loss_per_position(price=50.0, shares=400)
assert abs(max_loss - 700.0) < 0.01, f"기대 $700, 실제 {max_loss}"
print(f"[OK] max_loss_per_position: $50 × 400주 × 3.5% = ${max_loss:.2f}")

# ═══════════════════════════════════════════════════════════════
# T-R3: 일일 손실 한도 (DailyRiskState)
# ═══════════════════════════════════════════════════════════════
state = rm.new_daily_state()
assert state.loss_limit == -5_000.0   # 100k × 5%
assert rm.daily_loss_ok(state) is True
print(f"[OK] daily_loss_ok: 초기 상태 → True (한도 ${state.loss_limit:,.0f})")

# 손실 누적 → 한도 미달
state.close_position("NVDA", -3_000)
assert rm.daily_loss_ok(state) is True
state.close_position("AMD",  -2_001)
assert rm.daily_loss_ok(state) is False
assert state.blocked is True
assert "한도" in state.block_reason
print(f"[OK] daily_loss_ok: -$5,001 → False (차단, 이유: {state.block_reason})")

# ═══════════════════════════════════════════════════════════════
# T-R4: 동시 보유 종목 수 한도
# ═══════════════════════════════════════════════════════════════
state2 = rm.new_daily_state()
state2.open_position("NVDA")
state2.open_position("AMD")
state2.open_position("TSLA")
state2.open_position("META")
state2.open_position("SOXL")
assert state2.n_open == 5

ok, reason = rm.validate_entry("RKLB", 10.0, 100, state2)
assert ok is False and "한도" in reason
print(f"[OK] 동시 보유 한도 초과: {reason}")

# 청산 후 재진입 허용
state2.close_position("NVDA", 0)
assert state2.n_open == 4
ok2, _ = rm.validate_entry("RKLB", 10.0, 100, state2)
assert ok2 is True
print("[OK] 청산 후 재진입 허용")

# ═══════════════════════════════════════════════════════════════
# T-R5: 레짐 기반 진입 제한 (signals 레이어 출력 사용)
# ═══════════════════════════════════════════════════════════════
state3 = rm.new_daily_state()

bear_regime = {"qqq_regime": "bear", "qqq_chg_pct": -0.012}
ok, reason = rm.validate_entry("SOXL", 50.0, 100, state3, regime_features=bear_regime)
assert ok is False and "bear" in reason
print(f"[OK] 레짐 bear → 진입 거부 ({reason})")

bull_regime  = {"qqq_regime": "bull",    "qqq_chg_pct": 0.008}
neut_regime  = {"qqq_regime": "neutral", "qqq_chg_pct": 0.001}
ok_bull, _   = rm.validate_entry("SOXL", 50.0, 100, state3, regime_features=bull_regime)
ok_neut, _   = rm.validate_entry("SOXL", 50.0, 100, state3, regime_features=neut_regime)
assert ok_bull is True
assert ok_neut is True
print("[OK] 레짐 bull/neutral → 진입 허용")

# ═══════════════════════════════════════════════════════════════
# T-R6: 최소 주가 필터
# ═══════════════════════════════════════════════════════════════
state4 = rm.new_daily_state()
ok, reason = rm.validate_entry("PENNY", 3.0, 100, state4)
assert ok is False and "주가" in reason
print(f"[OK] 주가 $3 < 최소 $5 → 거부 ({reason})")

# ═══════════════════════════════════════════════════════════════
# T-R7: 유니버스 필터
# ═══════════════════════════════════════════════════════════════
prev_df = pd.DataFrame({
    "close":  [800.0, 50.0, 3.0,   200.0],
    "volume": [50e6,  2e6,  500e3, 100e3],
}, index=["NVDA", "RKLB", "PENNY", "ILLIQ"])

filtered = rm.filter_universe(["NVDA", "RKLB", "PENNY", "ILLIQ"], prev_df)
assert "NVDA"  in filtered
assert "RKLB"  in filtered
assert "PENNY" not in filtered   # 주가 미달
assert "ILLIQ" not in filtered   # 거래량 미달
print(f"[OK] filter_universe: {filtered} (PENNY/ILLIQ 제외)")

# ═══════════════════════════════════════════════════════════════
# T-R8: validate_entry 종합 우선순위
# ═══════════════════════════════════════════════════════════════
state5 = rm.new_daily_state()
# 일일 한도 초과 → 다른 조건보다 먼저 거부
state5.close_position("X", -6_000)
ok, reason = rm.validate_entry("NVDA", 800.0, 25, state5, regime_features=bull_regime)
assert ok is False and "한도" in reason
print(f"[OK] 일일 한도 우선 거부: {reason}")

print()
print("Phase 4 모든 테스트 통과")
