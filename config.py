"""
config.py
NASDAQ Intraday Quant System — 전체 파라미터 중앙 관리
"""
from zoneinfo import ZoneInfo

# ─────────────────────────────────────────
# 시간대
# ─────────────────────────────────────────
ET = ZoneInfo("America/New_York")

# ─────────────────────────────────────────
# ORB 전략 파라미터
# ─────────────────────────────────────────
TOP_N           = 5
TAKE_PROFIT     = 0.07       # 7% TP
STOP_LOSS_PCT   = 0.035      # 3.5% SL
ORB_BUFFER      = 0.0        # 돌파 버퍼 (0 = 정확히 고점 돌파)
VOL_SURGE_MULT  = 1.0        # OR 평균 거래량 배수 (1.0 = 이상이면 통과)
ENTRY_DEADLINE  = (13, 0)    # 진입 마감 ET 13:00
FORCE_CLOSE     = (15, 30)   # EOD 강제 청산 ET 15:30

# ─────────────────────────────────────────
# 유니버스 필터
# ─────────────────────────────────────────
MIN_PRICE       = 5.0        # 최소 주가 ($)
MIN_AVG_VOLUME  = 1_000_000  # 최소 일평균 거래량 (주)
MIN_OR_RANGE    = 0.0        # OR 범위 최소 (0 = 필터 없음)

# ─────────────────────────────────────────
# 리스크 관리
# ─────────────────────────────────────────
CAPITAL         = 100_000    # 초기 자본 ($)
MAX_DAILY_LOSS  = 0.05       # 일일 최대 손실 5% 초과 시 당일 매매 중단

# ─────────────────────────────────────────
# 체결 시뮬레이션 — 슬리피지 & 수수료
# ─────────────────────────────────────────
# 스프레드 티어 (bid-ask half-spread)
SPREAD_TIER: dict[str, list[str]] = {
    "large": [
        "NVDA", "AMD", "META", "TSLA", "AAPL", "AMZN", "MSFT", "NFLX",
        "GOOGL", "AVGO", "MRVL", "QCOM", "AMAT", "MU", "LRCX", "KLAC",
        "MELI", "PYPL", "INTU", "ISRG", "PANW", "CDNS", "FTNT", "REGN",
        "VRTX", "GILD", "SNPS", "ADSK",
    ],
    "etf": [
        "SOXL", "TQQQ", "UVXY", "SQQQ", "SPXL", "TECL", "FNGU", "LABU",
        "DPST", "NAIL", "TNA", "CURE", "HIBL", "WEBL", "BULZ", "RETL",
        "PILL", "DFEN", "UDOW",
    ],
    # "mid": 위 두 그룹에 포함되지 않은 모든 종목
}

# half-spread (매수 시 기준가 대비 불리하게 체결되는 비율)
SPREAD_BPS: dict[str, float] = {
    "large": 0.0001,   # 1bp
    "etf":   0.0003,   # 3bp
    "mid":   0.0005,   # 5bp
}

# 시장충격 계수: order_value / avg_daily_value 비율당 슬리피지
MARKET_IMPACT_COEF = 0.10   # 일거래대금의 0.1% 주문 → +0.01% 슬리피지

# Alpaca 수수료 모델
COMMISSION_PER_SHARE = 0.005   # $0.005/주
COMMISSION_MIN       = 1.00    # 최소 $1.00

# ─────────────────────────────────────────
# 유니버스 (SMAR 제거 완료)
# ─────────────────────────────────────────
UNIVERSE: list[str] = [
    # NASDAQ 100 대형주
    "NVDA", "AMD", "META", "TSLA", "AAPL", "AMZN", "MSFT", "NFLX", "GOOGL", "AVGO",
    "MRVL", "SMCI", "PLTR", "CRWD", "DDOG", "NET", "INTC", "QCOM", "AMAT", "MU",
    "LRCX", "KLAC", "MELI", "PYPL", "INTU", "ISRG", "PANW", "CDNS", "FTNT", "ABNB",
    "ZS", "REGN", "VRTX", "GILD", "MRNA", "SNPS", "ADSK", "TEAM", "WDAY", "OKTA",
    "ZM", "DOCU", "BILL", "HUBS", "SHOP", "SE", "BIDU", "JD", "PDD",
    "COIN", "HOOD", "SOFI", "AFRM", "UPST", "RIVN", "NIO", "LCID", "XPEV", "LI",
    # 고변동성 레버리지 ETF
    "SOXL", "TQQQ", "UVXY", "SQQQ", "SPXL", "TECL", "FNGU", "LABU", "DPST", "NAIL",
    "TNA", "CURE", "HIBL", "WEBL", "BULZ", "RETL", "PILL", "DFEN", "UDOW",
    # 고변동성 개별주
    "SOUN", "RKLB", "JOBY", "ACHR", "WOLF", "CELH", "BYND", "PLUG", "CHPT", "BE",
]


def get_spread_tier(ticker: str) -> str:
    """종목의 스프레드 티어 반환"""
    if ticker in SPREAD_TIER["large"]:
        return "large"
    if ticker in SPREAD_TIER["etf"]:
        return "etf"
    return "mid"
