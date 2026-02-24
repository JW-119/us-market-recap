import os
from dotenv import load_dotenv

load_dotenv()


def _get(key, default=""):
    """환경변수 → st.secrets → 기본값 순으로 조회."""
    val = os.getenv(key)
    if val:
        return val
    try:
        import streamlit as st
        return st.secrets.get(key, default)
    except Exception:
        return default


TELEGRAM_BOT_TOKEN = _get("TELEGRAM_BOT_TOKEN")
CHANNEL_ID = _get("CHANNEL_ID")

# ── Groq (섹터 뉴스 LLM 요약) ──
GROQ_API_KEY = _get("GROQ_API_KEY")

# ── GitHub Archive ──
GITHUB_TOKEN = _get("GITHUB_TOKEN")
GITHUB_REPO = _get("GITHUB_REPO", "JW-119/us-market-recap")

# ── 주요 지수 ──
INDICES = {
    "^GSPC": "S&P 500",
    "^IXIC": "NASDAQ",
    "^DJI": "Dow Jones",
    "^RUT": "Russell 2000",
    "^VIX": "VIX",
}

# ── 섹터 ETF (11개 SPDR) ──
SECTOR_ETFS = {
    "XLK": "기술",
    "XLF": "금융",
    "XLE": "에너지",
    "XLV": "헬스케어",
    "XLI": "산업재",
    "XLY": "임의소비재",
    "XLP": "필수소비재",
    "XLB": "소재",
    "XLU": "유틸리티",
    "XLRE": "부동산",
    "XLC": "커뮤니케이션",
}

# ── 시총 상위 50개 대형주 ──
MAJOR_STOCKS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "BRK-B",
    "AVGO", "JPM", "LLY", "V", "UNH", "MA", "XOM", "COST", "HD",
    "PG", "JNJ", "ABBV", "NFLX", "CRM", "BAC", "AMD", "ORCL",
    "KO", "CVX", "MRK", "PEP", "TMO", "WMT", "ADBE", "ACN", "LIN",
    "CSCO", "MCD", "ABT", "PM", "TXN", "QCOM", "ISRG", "INTU",
    "AMGN", "GE", "AMAT", "CAT", "GS", "NOW", "MS", "NEE",
]

# ── Barchart New Highs ──
BARCHART_PAGE_URL = "https://www.barchart.com/stocks/highs-lows/highs?timeFrame=52w"
BARCHART_API_URL = "https://www.barchart.com/proxies/core-api/v1/quotes/get"
BARCHART_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}
NEW_HIGHS_TARGETS = [
    {"lists": "stocks.us.new_highs_lows.highs.overall.1y", "label": "52주 신고가"},
    {"lists": "stocks.us.new_highs_lows.highs.overall.3m", "label": "3개월 신고가"},
]
NEW_HIGHS_FIELDS = (
    "symbol,symbolName,marketCap,lastPrice,priceChange,percentChange,"
    "volume,percentChange1m,percentChange3m,percentChange1y,percentChangeYtd"
)
NEW_HIGHS_MIN_MARKET_CAP = 2_000_000_000
NEW_HIGHS_PAGE_SIZE = 500

# ── 섹터별 Google News 검색 쿼리 ──
SECTOR_NEWS_QUERIES = {
    "XLK": "technology sector stocks",
    "XLF": "financial sector stocks banks",
    "XLE": "energy sector oil gas stocks",
    "XLV": "healthcare sector pharma biotech stocks",
    "XLI": "industrial sector stocks manufacturing",
    "XLY": "consumer discretionary sector stocks retail",
    "XLP": "consumer staples sector stocks",
    "XLB": "materials sector stocks mining chemicals",
    "XLU": "utilities sector stocks",
    "XLRE": "real estate sector REIT stocks",
    "XLC": "communication services sector stocks media",
}
