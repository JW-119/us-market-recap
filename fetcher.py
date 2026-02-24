import yfinance as yf
import pandas as pd
from config import INDICES, SECTOR_ETFS, MAJOR_STOCKS


def get_market_date():
    """마지막 거래일 문자열 (YYYY-MM-DD)."""
    sp = yf.download("^GSPC", period="5d", progress=False)
    if sp.empty:
        return None
    return str(sp.index[-1].date())


def fetch_indices():
    """주요 지수별 종가·전일대비·등락률 DataFrame 반환."""
    tickers = list(INDICES.keys())
    data = yf.download(tickers, period="5d", progress=False, group_by="ticker")

    rows = []
    for tkr, name in INDICES.items():
        try:
            if len(tickers) == 1:
                df = data
            else:
                df = data[tkr]
            df = df.dropna(subset=["Close"])
            if len(df) < 2:
                continue
            close = float(df["Close"].iloc[-1])
            prev = float(df["Close"].iloc[-2])
            chg = close - prev
            pct = chg / prev * 100
            rows.append({
                "이름": name,
                "티커": tkr,
                "종가": round(close, 2),
                "변동": round(chg, 2),
                "등락률": round(pct, 2),
            })
        except Exception:
            continue
    return pd.DataFrame(rows)


def fetch_sectors():
    """섹터 ETF별 등락률 DataFrame 반환 (등락률 내림차순)."""
    tickers = list(SECTOR_ETFS.keys())
    data = yf.download(tickers, period="5d", progress=False, group_by="ticker")

    rows = []
    for tkr, name in SECTOR_ETFS.items():
        try:
            df = data[tkr].dropna(subset=["Close"])
            if len(df) < 2:
                continue
            close = float(df["Close"].iloc[-1])
            prev = float(df["Close"].iloc[-2])
            pct = (close - prev) / prev * 100
            rows.append({
                "섹터": name,
                "티커": tkr,
                "종가": round(close, 2),
                "등락률": round(pct, 2),
            })
        except Exception:
            continue
    result = pd.DataFrame(rows)
    if not result.empty:
        result = result.sort_values("등락률", ascending=False).reset_index(drop=True)
    return result


def fetch_top_movers(top_n=10):
    """대형주 상승/하락 Top N → (gainers_df, losers_df) 튜플."""
    tickers = MAJOR_STOCKS
    data = yf.download(tickers, period="5d", progress=False, group_by="ticker")

    rows = []
    for tkr in tickers:
        try:
            df = data[tkr].dropna(subset=["Close"])
            if len(df) < 2:
                continue
            close = float(df["Close"].iloc[-1])
            prev = float(df["Close"].iloc[-2])
            chg = close - prev
            pct = chg / prev * 100
            rows.append({
                "티커": tkr,
                "종가": round(close, 2),
                "변동": round(chg, 2),
                "등락률": round(pct, 2),
            })
        except Exception:
            continue

    all_df = pd.DataFrame(rows)
    if all_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    all_df = all_df.sort_values("등락률", ascending=False).reset_index(drop=True)
    gainers = all_df.head(top_n).reset_index(drop=True)
    losers = all_df.tail(top_n).sort_values("등락률").reset_index(drop=True)
    return gainers, losers


def fetch_index_history(ticker, period="1mo"):
    """캔들차트용 OHLCV DataFrame (날짜 인덱스)."""
    df = yf.download(ticker, period=period, progress=False)
    if df.empty:
        return df
    # MultiIndex columns → flatten
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df
