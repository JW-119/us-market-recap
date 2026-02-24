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
