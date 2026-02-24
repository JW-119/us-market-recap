from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from fetcher import (
    fetch_indices,
    fetch_sectors,
    fetch_top_movers,
    fetch_index_history,
    fetch_sector_news,
    fetch_weekly_earnings,
    fetch_fear_greed,
    fetch_new_highs,
    get_market_date,
)
from archive import save_daily_snapshot, list_archive_dates, load_snapshot
from config import INDICES

_TZ_ET = ZoneInfo("America/New_York")
_TZ_KST = ZoneInfo("Asia/Seoul")


def _news_update_slot():
    """뉴스 업데이트 스케줄 슬롯. 값이 바뀌면 캐시 갱신.

    업데이트 시점: 장 시작(09:30 ET), 장 마감(16:00 ET), 한국 아침(08:00 KST)
    """
    now = datetime.now(timezone.utc)
    now_et = now.astimezone(_TZ_ET)
    today_et = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_et = today_et - timedelta(days=1)

    now_kst = now.astimezone(_TZ_KST)
    today_kst = now_kst.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_kst = today_kst - timedelta(days=1)

    slots = [
        today_et.replace(hour=9, minute=30),       # 장 시초
        today_et.replace(hour=16, minute=0),        # 장 마감
        today_kst.replace(hour=8, minute=0),        # 한국 아침
        yesterday_et.replace(hour=9, minute=30),
        yesterday_et.replace(hour=16, minute=0),
        yesterday_kst.replace(hour=8, minute=0),
    ]

    slots_utc = [s.astimezone(timezone.utc) for s in slots]
    past = [s for s in slots_utc if s <= now]
    return str(max(past)) if past else str(min(slots_utc))

st.set_page_config(page_title="US Market Recap", page_icon="📊", layout="wide")


# ── 사이드바 ──
st.sidebar.title("설정")
chart_period = st.sidebar.selectbox(
    "차트 기간",
    ["5d", "1mo", "3mo", "6mo", "1y"],
    index=1,
    format_func=lambda x: {
        "5d": "5일", "1mo": "1개월", "3mo": "3개월",
        "6mo": "6개월", "1y": "1년",
    }[x],
)
top_n = st.sidebar.slider("Top Movers 수", min_value=3, max_value=20, value=10)
if st.sidebar.button("🔄 새로고침"):
    st.cache_data.clear()

# ── 사이드바: 아카이브 ──
st.sidebar.divider()
st.sidebar.subheader("📁 아카이브")

archive_dates = list_archive_dates()
archive_options = ["📡 실시간"] + archive_dates
selected = st.sidebar.radio(
    "조회 날짜",
    archive_options,
    index=0,
    label_visibility="collapsed",
)
is_live = selected == "📡 실시간"


# ── 캐시 래퍼 ──
@st.cache_data(ttl=300)
def cached_indices():
    return fetch_indices()


@st.cache_data(ttl=300)
def cached_sectors():
    return fetch_sectors()


@st.cache_data(ttl=300)
def cached_movers(n):
    return fetch_top_movers(n)


@st.cache_data(ttl=300)
def cached_history(ticker, period):
    return fetch_index_history(ticker, period)


@st.cache_data(ttl=300)
def cached_date():
    return get_market_date()


@st.cache_data(ttl=300)
def cached_fear_greed():
    return fetch_fear_greed()


@st.cache_data(ttl=86400)
def cached_sector_news(slot):
    return fetch_sector_news()


@st.cache_data(ttl=3600)
def cached_weekly_earnings():
    return fetch_weekly_earnings()


@st.cache_data(ttl=300)
def cached_new_highs():
    return fetch_new_highs()


# ── 데이터 로드 ──
if is_live:
    market_date = cached_date()
    indices = cached_indices()
    sectors = cached_sectors()
    gainers, losers = cached_movers(top_n)
    fg = cached_fear_greed()
    sector_news = cached_sector_news(_news_update_slot())
    new_highs = cached_new_highs()
    earnings_df = cached_weekly_earnings()

    # 자동 아카이브 저장
    if market_date and not indices.empty:
        snapshot = {
            "market_date": market_date,
            "indices": indices.to_dict(orient="records"),
            "fear_greed": fg,
            "sectors": sectors.to_dict(orient="records") if not sectors.empty else [],
            "sector_news": sector_news,
            "new_highs": {
                k: v.to_dict(orient="records") if isinstance(v, pd.DataFrame) and not v.empty else []
                for k, v in new_highs.items()
            },
            "earnings": earnings_df.to_dict(orient="records") if not earnings_df.empty else [],
            "gainers": gainers.to_dict(orient="records") if not gainers.empty else [],
            "losers": losers.to_dict(orient="records") if not losers.empty else [],
        }
        save_daily_snapshot(snapshot)
else:
    # 아카이브 모드
    snap = load_snapshot(selected)
    if snap is None:
        st.error(f"'{selected}' 아카이브를 불러올 수 없습니다.")
        st.stop()

    market_date = snap["market_date"]
    indices = pd.DataFrame(snap.get("indices", []))
    sectors = pd.DataFrame(snap.get("sectors", []))
    fg = snap.get("fear_greed")
    sector_news = snap.get("sector_news", {})
    new_highs = {k: pd.DataFrame(v) for k, v in snap.get("new_highs", {}).items()}
    earnings_df = pd.DataFrame(snap.get("earnings", []))
    gainers = pd.DataFrame(snap.get("gainers", []))
    losers = pd.DataFrame(snap.get("losers", []))


# ── 제목 ──
mode_label = "" if is_live else " (아카이브)"
st.title(f"📊 미국 시장 시황 — {market_date or ''}{mode_label}")


# ── 1. 지수 요약 (st.metric) + Fear & Greed ──
st.subheader("주요 지수")
if not indices.empty:
    n_cols = len(indices) + (1 if fg else 0)
    cols = st.columns(n_cols)
    for col, (_, row) in zip(cols, indices.iterrows()):
        delta_color = "inverse" if row["티커"] == "^VIX" else "normal"
        col.metric(
            label=row["이름"],
            value=f"{row['종가']:,.2f}",
            delta=f"{row['변동']:+,.2f} ({row['등락률']:+.2f}%)",
            delta_color=delta_color,
        )
    if fg:
        cols[-1].metric(
            label=f"Fear & Greed ({fg['rating']})",
            value=fg["score"],
            delta=f"{fg['change']:+d}",
            delta_color="normal",
        )

st.divider()


# ── 2. 섹터 수평 바차트 ──
st.subheader("섹터별 등락률")
if not sectors.empty:
    s = sectors.sort_values("등락률")
    colors = ["#ef4444" if v < 0 else "#22c55e" for v in s["등락률"]]
    fig = go.Figure(go.Bar(
        x=s["등락률"],
        y=s["섹터"],
        orientation="h",
        marker_color=colors,
        text=[f"{v:+.2f}%" for v in s["등락률"]],
        textposition="outside",
    ))
    fig.update_layout(
        xaxis_title="등락률 (%)",
        yaxis_title="",
        height=400,
        margin=dict(l=0, r=40, t=10, b=30),
    )
    st.plotly_chart(fig, width="stretch")

st.divider()


# ── 3. 섹터별 주요 이슈 ──
st.subheader("섹터별 주요 이슈")
if not sectors.empty:
    # 등락 이유 요약 표
    reason_rows = []
    for _, row in sectors.iterrows():
        sec_name = row["섹터"]
        pct = row["등락률"]
        news_list = sector_news.get(sec_name, [])
        summaries = [n["summary"] for n in news_list if n.get("summary")]
        reason = " / ".join(summaries) if summaries else "-"
        reason_rows.append({
            "섹터": sec_name,
            "등락률": f"{pct:+.2f}%",
            "등락 이유": reason,
        })

    if reason_rows:
        reason_df = pd.DataFrame(reason_rows)
        reason_df["등락 이유"] = reason_df["등락 이유"].apply(
            lambda x: x.replace(" / ", "<br>") if x != "-" else x
        )
        html = reason_df.to_html(index=False, escape=False)
        st.markdown(
            "<style>"
            "#reason-table table { width: 100%; border-collapse: collapse; }"
            "#reason-table th, #reason-table td { "
            "  border: 1px solid #444; padding: 6px 10px; text-align: left; "
            "  word-break: keep-all; white-space: normal; }"
            "#reason-table th { background: #262730; }"
            "#reason-table td:nth-child(1) { width: 80px; white-space: nowrap; }"
            "#reason-table td:nth-child(2) { width: 70px; white-space: nowrap; text-align: right; }"
            "</style>"
            f'<div id="reason-table">{html}</div>',
            unsafe_allow_html=True,
        )

    # 섹터별 뉴스 상세
    for _, row in sectors.iterrows():
        sec_name = row["섹터"]
        pct = row["등락률"]
        sign = "+" if pct >= 0 else ""
        news_list = sector_news.get(sec_name, [])
        with st.expander(f"{sec_name}  ({sign}{pct:.2f}%)"):
            if news_list:
                for n in news_list:
                    src = f" — _{n['publisher']}_" if n["publisher"] else ""
                    title = n["title"]
                    if n.get("url"):
                        title = f"[{title}]({n['url']})"
                    st.markdown(f"- **{title}**{src}")
                    if n["summary"]:
                        st.caption(n["summary"])
            else:
                st.write("관련 뉴스를 찾을 수 없습니다.")

st.divider()


# ── 4. 신고가 종목 ──
st.subheader("신고가 종목")
tab_labels = []
for target in ["52주 신고가", "3개월 신고가"]:
    df = new_highs.get(target, pd.DataFrame())
    tab_labels.append(f"{target} ({len(df)})")

nh_tabs = st.tabs(tab_labels)
for tab, target in zip(nh_tabs, ["52주 신고가", "3개월 신고가"]):
    with tab:
        df = new_highs.get(target, pd.DataFrame())
        if df.empty:
            st.info(f"{target} 데이터가 없습니다.")
        else:
            st.dataframe(df, hide_index=True, width="stretch")

st.divider()


# ── 5. 이번 주 실적 발표 일정 ──
st.subheader("📅 이번 주 실적 발표 일정")
if earnings_df.empty:
    st.info("이번 주 예정된 실적 발표가 없습니다.")
else:
    st.dataframe(earnings_df, hide_index=True, width="stretch")

st.divider()


# ── 6. 지수 추이 (캔들스틱 탭) — 실시간 모드에서만 ──
if is_live:
    st.subheader("지수 추이")
    index_names = list(INDICES.values())
    index_tickers = list(INDICES.keys())
    tabs = st.tabs(index_names)

    for tab, tkr, name in zip(tabs, index_tickers, index_names):
        with tab:
            hist = cached_history(tkr, chart_period)
            if hist.empty:
                st.warning(f"{name} 데이터를 불러올 수 없습니다.")
                continue

            fig = go.Figure(go.Candlestick(
                x=hist.index,
                open=hist["Open"],
                high=hist["High"],
                low=hist["Low"],
                close=hist["Close"],
            ))
            fig.update_layout(
                title=name,
                xaxis_rangeslider_visible=False,
                height=420,
                margin=dict(l=0, r=0, t=40, b=30),
            )
            st.plotly_chart(fig, width="stretch")

    st.divider()


# ── 7. Top Movers ──
st.subheader(f"Top {top_n} Movers")
col_gain, col_lose = st.columns(2)

with col_gain:
    st.markdown("**🔺 상승**")
    if not gainers.empty:
        display = gainers[["종목명", "티커", "종가", "등락률"]].copy()
        display["등락률"] = display["등락률"].apply(lambda x: f"+{x:.2f}%")
        display["종가"] = display["종가"].apply(lambda x: f"{x:,.2f}")
        st.dataframe(display, hide_index=True, width="stretch")

with col_lose:
    st.markdown("**🔻 하락**")
    if not losers.empty:
        display = losers[["종목명", "티커", "종가", "등락률"]].copy()
        display["등락률"] = display["등락률"].apply(lambda x: f"{x:.2f}%")
        display["종가"] = display["종가"].apply(lambda x: f"{x:,.2f}")
        st.dataframe(display, hide_index=True, width="stretch")
