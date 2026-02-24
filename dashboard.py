import streamlit as st
import plotly.graph_objects as go
from fetcher import (
    fetch_indices,
    fetch_sectors,
    fetch_top_movers,
    fetch_index_history,
    get_market_date,
)
from config import INDICES

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


# ── 데이터 로드 ──
market_date = cached_date()
st.title(f"📊 미국 시장 시황 — {market_date or ''}")

indices = cached_indices()
sectors = cached_sectors()
gainers, losers = cached_movers(top_n)


# ── 1. 지수 요약 (st.metric) ──
st.subheader("주요 지수")
if not indices.empty:
    cols = st.columns(len(indices))
    for col, (_, row) in zip(cols, indices.iterrows()):
        # VIX는 inverse 색상 (상승=빨강, 하락=초록)
        delta_color = "inverse" if row["티커"] == "^VIX" else "normal"
        col.metric(
            label=row["이름"],
            value=f"{row['종가']:,.2f}",
            delta=f"{row['변동']:+,.2f} ({row['등락률']:+.2f}%)",
            delta_color=delta_color,
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
    st.plotly_chart(fig, use_container_width=True)

st.divider()


# ── 3. 지수 추이 (캔들스틱 탭) ──
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
        st.plotly_chart(fig, use_container_width=True)

st.divider()


# ── 4. Top Movers ──
st.subheader(f"Top {top_n} Movers")
col_gain, col_lose = st.columns(2)

with col_gain:
    st.markdown("**🔺 상승**")
    if not gainers.empty:
        display = gainers[["티커", "종가", "변동", "등락률"]].copy()
        display["등락률"] = display["등락률"].apply(lambda x: f"+{x:.2f}%")
        display["변동"] = display["변동"].apply(lambda x: f"+{x:,.2f}")
        display["종가"] = display["종가"].apply(lambda x: f"{x:,.2f}")
        st.dataframe(display, hide_index=True, use_container_width=True)

with col_lose:
    st.markdown("**🔻 하락**")
    if not losers.empty:
        display = losers[["티커", "종가", "변동", "등락률"]].copy()
        display["등락률"] = display["등락률"].apply(lambda x: f"{x:.2f}%")
        display["변동"] = display["변동"].apply(lambda x: f"{x:,.2f}")
        display["종가"] = display["종가"].apply(lambda x: f"{x:,.2f}")
        st.dataframe(display, hide_index=True, use_container_width=True)
