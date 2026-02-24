import time
from urllib.parse import unquote

import yfinance as yf
import pandas as pd
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from config import (
    INDICES, SECTOR_ETFS, MAJOR_STOCKS,
    BARCHART_PAGE_URL, BARCHART_API_URL, BARCHART_HEADERS,
    NEW_HIGHS_TARGETS, NEW_HIGHS_FIELDS, NEW_HIGHS_MIN_MARKET_CAP,
    NEW_HIGHS_PAGE_SIZE,
)


def get_market_date():
    """마지막 거래일 문자열 (YYYY-MM-DD)."""
    sp = yf.download("^GSPC", period="5d", progress=False)
    if sp.empty:
        return None
    return str(sp.index[-1].date())


def fetch_fear_greed():
    """CNN Fear & Greed Index 현재 점수·등급·전일대비 변동 반환."""
    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Referer": "https://www.cnn.com/markets/fear-and-greed",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        fg = resp.json()["fear_and_greed"]
        score = round(fg["score"])
        prev = round(fg.get("previous_close", score))
        rating_map = {
            "extreme fear": "극단적 공포",
            "fear": "공포",
            "neutral": "중립",
            "greed": "탐욕",
            "extreme greed": "극단적 탐욕",
        }
        return {
            "score": score,
            "prev": prev,
            "change": score - prev,
            "rating": rating_map.get(fg["rating"], fg["rating"]),
        }
    except Exception:
        return None


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


def _get_stock_name(tkr):
    """yfinance에서 종목 shortName 조회. 실패 시 티커 반환."""
    try:
        info = yf.Ticker(tkr).info
        return tkr, info.get("shortName", tkr)
    except Exception:
        return tkr, tkr


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

    # 상위/하위 종목에 대해서만 이름 조회 (병렬)
    need_names = set(gainers["티커"]) | set(losers["티커"])
    with ThreadPoolExecutor(max_workers=10) as pool:
        name_map = dict(pool.map(_get_stock_name, need_names))

    for df in (gainers, losers):
        df.insert(0, "종목명", df["티커"].map(name_map))

    return gainers, losers


def _translate_text(text):
    """텍스트를 한글로 번역. 실패 시 원문 반환."""
    if not text:
        return text
    try:
        from deep_translator import GoogleTranslator
        return GoogleTranslator(source="en", target="ko").translate(text)
    except Exception:
        return text


_NEWS_BLOCKED_PUBLISHERS = {"MT Newswires", "Barchart", "Barrons.com", "Barron's"}


def fetch_sector_news():
    """섹터 ETF별 최근 뉴스 3개씩 수집 → {섹터명: [{title, summary, publisher, url}, ...]}."""
    sector_map = {tkr: name for tkr, name in SECTOR_ETFS.items()}

    def _get_news(tkr):
        try:
            items = yf.Ticker(tkr).news or []
            results = []
            for item in items[:10]:
                content = item.get("content", {})
                title = content.get("title", "")
                summary = content.get("summary", "")
                provider = content.get("provider", {})
                publisher = provider.get("displayName", "") if isinstance(provider, dict) else ""
                click_through = content.get("clickThroughUrl", {})
                url = click_through.get("url", "") if isinstance(click_through, dict) else ""
                if not title or not url:
                    continue
                if publisher in _NEWS_BLOCKED_PUBLISHERS:
                    continue
                results.append({
                    "title": title,
                    "summary": summary,
                    "publisher": publisher,
                    "url": url,
                })
                if len(results) >= 3:
                    break
            return tkr, results
        except Exception:
            return tkr, []

    # 1) 뉴스 수집
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(_get_news, tkr): tkr for tkr in sector_map}
        output = {}
        for future in futures:
            tkr, news_list = future.result()
            output[sector_map[tkr]] = news_list

    # 2) 번역 (title + summary 병렬 처리)
    all_texts = []
    text_map = []  # (sector, idx, field)
    for sector, news_list in output.items():
        for i, n in enumerate(news_list):
            if n["title"]:
                all_texts.append(n["title"])
                text_map.append((sector, i, "title"))
            if n["summary"]:
                all_texts.append(n["summary"])
                text_map.append((sector, i, "summary"))

    if all_texts:
        with ThreadPoolExecutor(max_workers=10) as pool:
            translated = list(pool.map(_translate_text, all_texts))
        for (sector, i, field), translated_text in zip(text_map, translated):
            output[sector][i][field] = translated_text

    return output


def fetch_weekly_earnings():
    """MAJOR_STOCKS 중 이번 주 실적 발표 예정 종목 DataFrame 반환."""
    today = datetime.now()
    # 이번 주 월요일 ~ 일요일
    monday = today - timedelta(days=today.weekday())
    sunday = monday + timedelta(days=6)
    mon_date = monday.date()
    sun_date = sunday.date()

    def _get_earnings(tkr):
        try:
            cal = yf.Ticker(tkr).calendar
            if cal is None or not isinstance(cal, dict):
                return None
            earnings_dates = cal.get("Earnings Date", [])
            if not earnings_dates:
                return None
            for ed in earnings_dates:
                if hasattr(ed, "date"):
                    ed_date = ed.date()
                else:
                    ed_date = pd.Timestamp(ed).date()
                if mon_date <= ed_date <= sun_date:
                    eps_avg = cal.get("Earnings Average", None)
                    rev_avg = cal.get("Revenue Average", None)
                    return {
                        "티커": tkr,
                        "발표일": str(ed_date),
                        "EPS 예상": round(eps_avg, 2) if eps_avg else "-",
                        "매출 예상": f"{rev_avg / 1e9:.1f}B" if rev_avg else "-",
                    }
            return None
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=10) as pool:
        results = list(pool.map(_get_earnings, MAJOR_STOCKS))

    rows = [r for r in results if r is not None]
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("발표일").reset_index(drop=True)
    return df


def fetch_index_history(ticker, period="1mo"):
    """캔들차트용 OHLCV DataFrame (날짜 인덱스)."""
    df = yf.download(ticker, period=period, progress=False)
    if df.empty:
        return df
    # MultiIndex columns → flatten
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df


# ── Barchart New Highs ──

def _barchart_session() -> tuple[requests.Session, dict]:
    """Barchart 페이지에 접속하여 XSRF 토큰이 포함된 세션을 반환."""
    session = requests.Session()
    headers = dict(BARCHART_HEADERS)
    headers["Referer"] = BARCHART_PAGE_URL

    resp = session.get(BARCHART_PAGE_URL, headers=headers, timeout=30)
    resp.raise_for_status()

    xsrf_token = session.cookies.get("XSRF-TOKEN")
    if not xsrf_token:
        raise RuntimeError("XSRF-TOKEN 쿠키를 찾을 수 없습니다.")
    headers["X-XSRF-TOKEN"] = unquote(xsrf_token)
    return session, headers


def _barchart_fetch_page(session, headers, lists_value, page):
    """API에서 한 페이지의 데이터를 가져옴."""
    params = {
        "lists": lists_value,
        "fields": NEW_HIGHS_FIELDS,
        "meta": "field.shortName,field.type,field.description,lists.lastUpdate",
        "page": str(page),
        "limit": str(NEW_HIGHS_PAGE_SIZE),
        "raw": "1",
    }
    resp = session.get(BARCHART_API_URL, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _format_market_cap(value):
    """시가총액을 읽기 좋은 형식으로 변환 (예: 2.5T, 150.3B)."""
    if pd.isna(value) or value == 0:
        return "-"
    if value >= 1e12:
        return f"{value / 1e12:.1f}T"
    if value >= 1e9:
        return f"{value / 1e9:.1f}B"
    if value >= 1e6:
        return f"{value / 1e6:.0f}M"
    return str(value)


def _fetch_single_target(session, headers, lists_value, label):
    """하나의 타겟(52주/3개월)에 대해 전체 페이지를 수집하여 DataFrame 반환."""
    all_records = []
    page = 1

    field_names = [f.strip() for f in NEW_HIGHS_FIELDS.split(",")]

    while True:
        data = _barchart_fetch_page(session, headers, lists_value, page)
        records = data.get("data", [])
        if not records:
            break

        for record in records:
            raw = record.get("raw", record)
            row = {f: raw.get(f, "") for f in field_names}
            all_records.append(row)

        total = data.get("count", 0)
        fetched = page * NEW_HIGHS_PAGE_SIZE
        if fetched >= total or len(records) < NEW_HIGHS_PAGE_SIZE:
            break
        page += 1
        time.sleep(0.5)

    if not all_records:
        return pd.DataFrame()

    df = pd.DataFrame(all_records)

    # 시총 숫자 변환 + 필터링 ($2B 이상)
    df["marketCap"] = pd.to_numeric(df["marketCap"], errors="coerce")
    df = df[df["marketCap"] >= NEW_HIGHS_MIN_MARKET_CAP].copy()

    if df.empty:
        return df

    # 시총 내림차순 정렬
    df = df.sort_values("marketCap", ascending=False).reset_index(drop=True)

    # 퍼센트 필드 ×100 변환
    pct_fields = [
        "percentChange", "percentChange1m", "percentChange3m",
        "percentChange1y", "percentChangeYtd",
    ]
    for f in pct_fields:
        if f in df.columns:
            df[f] = pd.to_numeric(df[f], errors="coerce") * 100
            df[f] = df[f].round(2)

    # 숫자 변환
    for f in ["lastPrice", "priceChange", "volume"]:
        if f in df.columns:
            df[f] = pd.to_numeric(df[f], errors="coerce")

    # 시총 포맷팅
    df["marketCap"] = df["marketCap"].apply(_format_market_cap)

    # 컬럼명 변환
    col_map = {
        "symbol": "Symbol",
        "symbolName": "Name",
        "marketCap": "Market Cap",
        "lastPrice": "Price",
        "priceChange": "Change",
        "percentChange": "Change%",
        "volume": "Volume",
        "percentChange1m": "1M%",
        "percentChange3m": "3M%",
        "percentChange1y": "52W%",
        "percentChangeYtd": "YTD%",
    }
    df = df.rename(columns=col_map)
    display_cols = [v for v in col_map.values() if v in df.columns]
    return df[display_cols].reset_index(drop=True)


def fetch_new_highs():
    """Barchart에서 52주/3개월 신고가 종목을 수집.

    Returns:
        dict: {"52주 신고가": DataFrame, "3개월 신고가": DataFrame}
    """
    try:
        session, headers = _barchart_session()
    except Exception:
        return {t["label"]: pd.DataFrame() for t in NEW_HIGHS_TARGETS}

    def _fetch(target):
        try:
            return target["label"], _fetch_single_target(
                session, headers, target["lists"], target["label"]
            )
        except Exception:
            return target["label"], pd.DataFrame()

    results = {}
    with ThreadPoolExecutor(max_workers=2) as pool:
        for label, df in pool.map(_fetch, NEW_HIGHS_TARGETS):
            results[label] = df

    return results
