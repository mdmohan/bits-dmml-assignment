"""
app.py — Olist E-Commerce Analytics Dashboard
================================================
Streamlit multi-page dashboard with three sections:
  1) BI Dashboards   — Sales, payments, delivery, reviews, sellers
  2) Analytics       — Deep-dive category, geography, seller analysis
  3) Observability   — Pipeline health, ingestion freshness, data quality

Run:
    streamlit run app.py --server.port 8501 --server.address 0.0.0.0
"""
from __future__ import annotations

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import yaml
from pathlib import Path

from db import run_query
import queries as Q

# ── Config ────────────────────────────────────────────────────────────────────
CFG_PATH = Path(__file__).parent / "config.yaml"
with open(CFG_PATH) as f:
    CFG = yaml.safe_load(f)

DB_CFG = CFG["database"]
DASH_CFG = CFG["dashboard"]

st.set_page_config(
    page_title=DASH_CFG["title"],
    page_icon="�",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Amazon.in Theme CSS + 3D Effects ─────────────────────────────────────────
st.markdown("""
<style>
/* ─── Amazon Color Palette ─── */
:root {
    --amazon-dark: #131921;
    --amazon-dark2: #232f3e;
    --amazon-orange: #ff9900;
    --amazon-orange-hover: #ffad33;
    --amazon-light: #febd69;
    --amazon-blue: #146eb4;
    --amazon-bg: #eaeded;
    --card-bg: #ffffff;
    --text-dark: #0f1111;
    --text-light: #565959;
}

/* ─── Global Background ─── */
.stApp {
    background: linear-gradient(180deg, #eaeded 0%, #f5f5f5 100%);
}

/* ─── Sidebar Amazon Dark Theme ─── */
section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, var(--amazon-dark) 0%, var(--amazon-dark2) 100%) !important;
    border-right: 3px solid var(--amazon-orange);
}
section[data-testid="stSidebar"] * {
    color: #ffffff !important;
}
section[data-testid="stSidebar"] .stRadio label:hover {
    color: var(--amazon-orange) !important;
}
section[data-testid="stSidebar"] button {
    background: var(--amazon-orange) !important;
    color: var(--amazon-dark) !important;
    border: none !important;
    border-radius: 8px !important;
    font-weight: 700 !important;
    transition: all 0.3s ease;
}
section[data-testid="stSidebar"] button:hover {
    background: var(--amazon-orange-hover) !important;
    transform: scale(1.05);
    box-shadow: 0 4px 15px rgba(255, 153, 0, 0.4);
}

/* ─── Header Banner ─── */
.amazon-header {
    background: linear-gradient(135deg, var(--amazon-dark) 0%, var(--amazon-dark2) 100%);
    padding: 20px 30px;
    border-radius: 12px;
    margin-bottom: 25px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    box-shadow: 0 8px 32px rgba(0,0,0,0.3);
    border: 1px solid rgba(255, 153, 0, 0.2);
}
.amazon-header h1 {
    color: #ffffff;
    margin: 0;
    font-size: 1.8rem;
    font-weight: 700;
    text-shadow: 0 2px 4px rgba(0,0,0,0.3);
}
.amazon-header .accent {
    color: var(--amazon-orange);
}
.amazon-header img {
    height: 50px;
    border-radius: 8px;
}

/* ─── 3D Metric Cards ─── */
[data-testid="stMetric"] {
    background: var(--card-bg);
    border-radius: 12px;
    padding: 18px 20px;
    box-shadow:
        0 4px 6px rgba(0, 0, 0, 0.07),
        0 10px 20px rgba(0, 0, 0, 0.04),
        0 1px 3px rgba(0, 0, 0, 0.08);
    border-left: 4px solid var(--amazon-orange);
    transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    transform: perspective(1000px) rotateX(0deg) rotateY(0deg);
}
[data-testid="stMetric"]:hover {
    transform: perspective(1000px) rotateX(-2deg) rotateY(2deg) translateY(-4px);
    box-shadow:
        0 14px 28px rgba(0, 0, 0, 0.12),
        0 10px 10px rgba(0, 0, 0, 0.08),
        0 0 20px rgba(255, 153, 0, 0.15);
    border-left-color: var(--amazon-orange-hover);
}
[data-testid="stMetricLabel"] {
    color: var(--text-light) !important;
    font-size: 0.85rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}
[data-testid="stMetricValue"] {
    color: var(--text-dark) !important;
    font-weight: 700;
}

/* ─── 3D Tab Styling ─── */
.stTabs [data-baseweb="tab-list"] {
    background: var(--amazon-dark2);
    border-radius: 10px;
    padding: 4px;
    gap: 4px;
    box-shadow: inset 0 2px 4px rgba(0,0,0,0.3);
}
.stTabs [data-baseweb="tab"] {
    color: #cccccc !important;
    border-radius: 8px;
    padding: 8px 16px;
    font-weight: 600;
    transition: all 0.3s ease;
}
.stTabs [data-baseweb="tab"]:hover {
    color: var(--amazon-orange) !important;
    background: rgba(255,153,0,0.1);
}
.stTabs [aria-selected="true"] {
    background: var(--amazon-orange) !important;
    color: var(--amazon-dark) !important;
    box-shadow: 0 4px 12px rgba(255, 153, 0, 0.4);
    transform: translateY(-1px);
}

/* ─── 3D Chart Containers ─── */
[data-testid="stPlotlyChart"], [data-testid="stDataFrame"] {
    background: var(--card-bg);
    border-radius: 12px;
    padding: 10px;
    box-shadow:
        0 4px 6px rgba(0,0,0,0.05),
        0 8px 24px rgba(0,0,0,0.08);
    transition: all 0.3s ease;
    border: 1px solid rgba(0,0,0,0.05);
}
[data-testid="stPlotlyChart"]:hover, [data-testid="stDataFrame"]:hover {
    box-shadow:
        0 8px 16px rgba(0,0,0,0.1),
        0 12px 40px rgba(0,0,0,0.12);
    transform: translateY(-2px);
}

/* ─── Subheaders ─── */
h2, h3 {
    color: var(--amazon-dark) !important;
    border-bottom: 2px solid var(--amazon-orange);
    padding-bottom: 8px;
    display: inline-block;
}

/* ─── Buttons & Interactive ─── */
.stButton > button {
    background: linear-gradient(180deg, #f7dfa5 0%, var(--amazon-light) 100%) !important;
    color: var(--text-dark) !important;
    border: 1px solid #a88734 !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
    box-shadow: 0 2px 5px rgba(0,0,0,0.15);
    transition: all 0.2s ease;
}
.stButton > button:hover {
    background: linear-gradient(180deg, var(--amazon-light) 0%, #f0c14b 100%) !important;
    box-shadow: 0 4px 12px rgba(255,153,0,0.3);
    transform: translateY(-1px);
}

/* ─── Dividers ─── */
hr {
    border: none;
    height: 2px;
    background: linear-gradient(90deg, transparent, var(--amazon-orange), transparent);
    margin: 20px 0;
}

/* ─── Sliders ─── */
.stSlider [data-baseweb="slider"] [role="slider"] {
    background: var(--amazon-orange) !important;
}
.stSlider [data-baseweb="slider"] [data-testid="stTickBarMin"],
.stSlider [data-baseweb="slider"] [data-testid="stTickBarMax"] {
    color: var(--text-light);
}

/* ─── Loading Spinner ─── */
.stSpinner > div {
    border-top-color: var(--amazon-orange) !important;
}

/* ─── GIF Pulse Animation for Header ─── */
@keyframes pulse-glow {
    0%, 100% { box-shadow: 0 0 10px rgba(255,153,0,0.3); }
    50% { box-shadow: 0 0 25px rgba(255,153,0,0.6); }
}
.gif-container {
    display: inline-block;
    border-radius: 10px;
    animation: pulse-glow 2s ease-in-out infinite;
}

/* ─── 3D Float Animation for Logo ─── */
@keyframes float3d {
    0%, 100% { transform: perspective(500px) rotateY(0deg) translateY(0px); }
    25% { transform: perspective(500px) rotateY(5deg) translateY(-5px); }
    75% { transform: perspective(500px) rotateY(-5deg) translateY(-3px); }
}
.float-3d {
    animation: float3d 4s ease-in-out infinite;
    display: inline-block;
}

/* ─── Scrollbar Styling ─── */
::-webkit-scrollbar { width: 8px; }
::-webkit-scrollbar-track { background: var(--amazon-bg); }
::-webkit-scrollbar-thumb { background: var(--amazon-dark2); border-radius: 4px; }
::-webkit-scrollbar-thumb:hover { background: var(--amazon-orange); }
</style>
""", unsafe_allow_html=True)

# ── Amazon-style Header with GIF ─────────────────────────────────────────────
st.markdown("""
<div class="amazon-header">
    <div>
        <h1><span class="accent">📊</span> Olist E-Commerce <span class="accent">Analytics</span></h1>
        <p style="color: #999; margin: 5px 0 0 0; font-size: 0.9rem;">
            Real-time insights • Data-driven decisions • Powered by AI
        </p>
    </div>
    <div class="gif-container">
        <img src="https://media.giphy.com/media/l46Cy1rHbQ92uuLXa/giphy.gif" 
             alt="analytics" style="height:60px; border-radius: 8px;">
    </div>
</div>
""", unsafe_allow_html=True)


# ── Helpers ───────────────────────────────────────────────────────────────────
@st.cache_data(ttl=DASH_CFG.get("refresh_interval_sec", 60))
def query(sql: str) -> pd.DataFrame:
    return run_query(DB_CFG, sql)


def metric_card(label: str, value, delta=None, delta_color="normal"):
    st.metric(label=label, value=value, delta=delta, delta_color=delta_color)


# ── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.markdown("""
<div class="float-3d" style="text-align:center; width:100%; padding: 10px 0;">
    <span style="font-size: 2.5rem;">🛒</span>
</div>
""", unsafe_allow_html=True)
st.sidebar.title("Olist Analytics")
st.sidebar.markdown('<p style="color: #ff9900 !important; font-size: 0.8rem; margin-top:-10px;">━━━━━━━━━━━━━━━━━━━━</p>', unsafe_allow_html=True)
page = st.sidebar.radio(
    "Navigate",
    ["🏠 Overview", "📈 BI Dashboards", "🔬 Analytics", "🔭 Observability"],
)

if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()

st.sidebar.markdown("---")
st.sidebar.caption(f"DB: {DB_CFG['host']}:{DB_CFG['port']}/{DB_CFG['dbname']}")

st.sidebar.markdown("""
<div style="margin-top: 30px; padding: 15px; background: rgba(255,153,0,0.08); 
            border-radius: 10px; border: 1px solid rgba(255,153,0,0.3);">
    <p style="color: #ff9900 !important; font-weight: 700; margin-bottom: 8px; font-size: 0.85rem;">
        👨‍💻 Developed by
    </p>
    <ul style="list-style: none; padding: 0; margin: 0; font-size: 0.8rem; line-height: 1.8;">
        <li>• Akshaya Kumar Joish</li>
        <li>• Anuj Kumar Srivastava</li>
        <li>• Chandan Panda</li>
        <li>• Murali Das Mohan</li>
    </ul>
    <p style="color: #999 !important; font-size: 0.75rem; margin-top: 10px; margin-bottom: 0;">
        📧 For any issues, contact the team above.
    </p>
</div>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════════
if page == "🏠 Overview":
    st.title("🏠 Executive Overview")
    st.markdown("Key performance indicators at a glance.")

    # ── Row 1: Primary KPIs ──────────────────────────────────────────────────
    col1, col2, col3, col4, col5, col6 = st.columns(6)

    with col1:
        df = query(Q.KPI_TOTAL_ORDERS)
        metric_card("Total Orders", f"{df.iloc[0, 0]:,.0f}")
    with col2:
        df = query(Q.KPI_TOTAL_REVENUE)
        metric_card("Total Revenue", f"R$ {df.iloc[0, 0]:,.2f}")
    with col3:
        df = query(Q.KPI_AVG_ORDER_VALUE)
        metric_card("Avg Order Value", f"R$ {df.iloc[0, 0]:,.2f}")
    with col4:
        df = query(Q.KPI_TOTAL_CUSTOMERS)
        metric_card("Customers", f"{df.iloc[0, 0]:,.0f}")
    with col5:
        df = query(Q.KPI_AVG_REVIEW_SCORE)
        metric_card("Avg Review", f"{df.iloc[0, 0]:.2f} / 5")
    with col6:
        df = query(Q.KPI_DELIVERY_ON_TIME_PCT)
        pct = df.iloc[0, 0]
        metric_card("On-Time Delivery", f"{pct:.1f}%")

    # ── Row 2: Secondary KPIs ────────────────────────────────────────────────
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        df = query(Q.KPI_TOTAL_SELLERS)
        metric_card("Active Sellers", f"{df.iloc[0, 0]:,.0f}")
    with col2:
        df = query(Q.KPI_AVG_DELIVERY_DELAY)
        val = df.iloc[0, 0]
        color = "inverse" if val > 0 else "normal"
        metric_card("Avg Delivery Delay", f"{val:.1f}h", delta_color=color)
    with col3:
        df = query(Q.KPI_TOTAL_PRODUCTS_SOLD)
        metric_card("Items Sold", f"{df.iloc[0, 0]:,.0f}")
    with col4:
        df = query(Q.KPI_REVENUE_MOM)
        if not df.empty:
            mom = df.iloc[0, 0]
            metric_card("Revenue MoM", f"{mom:+.1f}%",
                        delta=f"{mom:+.1f}%",
                        delta_color="normal" if mom >= 0 else "inverse")
        else:
            metric_card("Revenue MoM", "N/A")
    with col5:
        df = query(Q.LAST_ETL_RUN)
        if not df.empty:
            status = "🟢" if df.iloc[0]["all_success"] else "🔴"
            metric_card("Last ETL", f"{status} OK")
        else:
            metric_card("Last ETL", "⚪ No runs")

    st.markdown("---")

    # ── Row 3: Trend sparklines ──────────────────────────────────────────────
    col_l, col_m, col_r = st.columns(3)

    with col_l:
        st.subheader("📈 Orders (last 30 days)")
        df = query(Q.ORDERS_LAST30D)
        if not df.empty:
            fig = px.area(df, x="event_date", y="orders",
                          labels={"orders": "", "event_date": ""})
            fig.update_layout(height=200, margin=dict(l=0, r=0, t=10, b=0),
                              showlegend=False)
            fig.update_xaxes(showticklabels=False)
            st.plotly_chart(fig, use_container_width=True)

    with col_m:
        st.subheader("⭐ Review Score Trend")
        df = query(Q.REVIEW_SCORE_LAST6M)
        if not df.empty:
            df = df.sort_values("month")
            fig = px.line(df, x="month", y="avg_score", markers=True,
                          labels={"avg_score": "", "month": ""})
            fig.update_layout(height=200, margin=dict(l=0, r=0, t=10, b=0),
                              showlegend=False)
            fig.update_yaxes(range=[1, 5])
            fig.update_xaxes(showticklabels=False)
            st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.subheader("🚚 Delivery SLA Trend")
        df = query(Q.DELIVERY_SLA_LAST6M)
        if not df.empty:
            df = df.sort_values("month")
            fig = px.line(df, x="month", y="on_time_pct", markers=True,
                          labels={"on_time_pct": "%", "month": ""})
            fig.update_layout(height=200, margin=dict(l=0, r=0, t=10, b=0),
                              showlegend=False)
            fig.update_yaxes(range=[0, 100])
            fig.update_xaxes(showticklabels=False)
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # ── Row 4: Monthly revenue + Payment mix ─────────────────────────────────
    col_l, col_r = st.columns(2)
    with col_l:
        st.subheader("Monthly Revenue Trend")
        df = query(Q.MONTHLY_SALES)
        if not df.empty:
            fig = px.bar(df, x="month", y="revenue", text="orders",
                         labels={"revenue": "Revenue (R$)", "month": "Month"})
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)
    with col_r:
        st.subheader("Payment Mix")
        df = query(Q.PAYMENT_MIX)
        if not df.empty:
            fig = px.pie(df, values="total_value", names="payment_type",
                         hole=0.4)
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # ── Row 5: Top/Bottom Lists ──────────────────────────────────────────────
    col_l, col_m, col_r = st.columns(3)

    with col_l:
        st.subheader("🏆 Top 5 Categories")
        df = query(Q.TOP5_CATEGORIES_OVERVIEW)
        if not df.empty:
            fig = px.bar(df, x="revenue", y="category", orientation="h",
                         labels={"revenue": "Revenue (R$)", "category": ""})
            fig.update_layout(height=250, margin=dict(l=0, r=0, t=10, b=0),
                              yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig, use_container_width=True)

    with col_m:
        st.subheader("📍 Top 5 States")
        df = query(Q.TOP5_STATES_OVERVIEW)
        if not df.empty:
            fig = px.bar(df, x="orders", y="state", orientation="h",
                         labels={"orders": "Orders", "state": ""})
            fig.update_layout(height=250, margin=dict(l=0, r=0, t=10, b=0),
                              yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.subheader("⚠️ Worst 5 Sellers (On-Time)")
        df = query(Q.BOTTOM5_SELLERS_ONTIME)
        if not df.empty:
            df["label"] = df["seller_id"].str[:8] + "..." + " (" + df["seller_state"] + ")"
            fig = px.bar(df, x="on_time_pct", y="label", orientation="h",
                         color="on_time_pct", color_continuous_scale="RdYlGn",
                         labels={"on_time_pct": "On-Time %", "label": ""})
            fig.update_layout(height=250, margin=dict(l=0, r=0, t=10, b=0),
                              yaxis=dict(autorange="reversed"), showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # ── Row 6: Pipeline Status + Table Inventory ─────────────────────────────
    col_l, col_r = st.columns([1, 2])

    with col_l:
        st.subheader("🔄 Pipeline Status")
        df = query(Q.INGESTION_FRESHNESS)
        if not df.empty:
            warn_sec = DASH_CFG.get("ingestion_lag_warn_sec", 120)
            crit_sec = DASH_CFG.get("ingestion_lag_critical_sec", 300)
            fresh_count = len(df[df["lag_seconds"].notna() & (df["lag_seconds"] <= warn_sec)])
            warn_count = len(df[df["lag_seconds"].notna() & (df["lag_seconds"] > warn_sec) & (df["lag_seconds"] <= crit_sec)])
            stale_count = len(df[df["lag_seconds"].notna() & (df["lag_seconds"] > crit_sec)])
            st.markdown(f"""
            | Status | Tables |
            |--------|--------|
            | 🟢 Fresh | {fresh_count} |
            | 🟡 Warning | {warn_count} |
            | 🔴 Stale | {stale_count} |
            """)

    with col_r:
        st.subheader("📋 Warehouse Table Inventory")
        df = query(Q.TABLE_ROW_COUNTS)
        if not df.empty:
            fig = px.bar(df, x="table_name", y="row_count",
                         color="row_count", color_continuous_scale="Blues",
                         labels={"row_count": "Rows", "table_name": ""})
            fig.update_layout(height=280, xaxis_tickangle=-45,
                              margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: BI DASHBOARDS
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "📈 BI Dashboards":
    st.title("📈 BI Dashboards")

    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10, tab11, tab12, tab13, tab14, tab15 = st.tabs([
        "Daily Sales", "Payment Mix", "Delivery SLA", "Reviews", "Sellers",
        "Customers", "Products", "Geography", "Order Lifecycle",
        "Cohort Analysis", "Seller Deep Dive", "Installments",
        "Seasonality", "Basket Analysis", "Freight & Logistics"
    ])

    # ── Daily Sales ──────────────────────────────────────────────────────────
    with tab1:
        st.subheader("Daily Sales Performance")
        df = query(Q.DAILY_SALES)
        if not df.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(x=df["event_date"], y=df["revenue"],
                                 name="Revenue", yaxis="y"))
            fig.add_trace(go.Scatter(x=df["event_date"], y=df["orders"],
                                     name="Orders", yaxis="y2", mode="lines"))
            fig.update_layout(
                yaxis=dict(title="Revenue (R$)", side="left"),
                yaxis2=dict(title="Orders", side="right", overlaying="y"),
                height=450,
                legend=dict(x=0, y=1.1, orientation="h"),
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No sales data available.")

    # ── Payment Mix ──────────────────────────────────────────────────────────
    with tab2:
        st.subheader("Payment Methods Breakdown")
        col_l, col_r = st.columns(2)
        with col_l:
            df = query(Q.PAYMENT_MIX)
            if not df.empty:
                fig = px.bar(df, x="payment_type", y="total_value",
                             color="payment_type", text="txn_count",
                             labels={"total_value": "Total Value (R$)"})
                fig.update_layout(height=400, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
        with col_r:
            df_t = query(Q.PAYMENT_TREND)
            if not df_t.empty:
                fig = px.area(df_t, x="month", y="total_value",
                              color="payment_type",
                              labels={"total_value": "Value (R$)"})
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)

    # ── Delivery SLA ─────────────────────────────────────────────────────────
    with tab3:
        st.subheader("Delivery SLA Performance")
        col_l, col_r = st.columns(2)
        with col_l:
            df = query(Q.DELIVERY_SLA_DISTRIBUTION)
            if not df.empty:
                fig = px.bar(df, x="sla_bucket", y="deliveries",
                             color="sla_bucket",
                             labels={"deliveries": "Deliveries"})
                fig.update_layout(height=400, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
        with col_r:
            df = query(Q.DELIVERY_MONTHLY_TREND)
            if not df.empty:
                fig = go.Figure()
                fig.add_trace(go.Bar(x=df["month"], y=df["avg_delay_hours"],
                                     name="Avg Delay (h)"))
                fig.add_trace(go.Scatter(x=df["month"], y=df["on_time_pct"],
                                         name="On-Time %", yaxis="y2", mode="lines+markers"))
                fig.update_layout(
                    yaxis=dict(title="Avg Delay (hours)"),
                    yaxis2=dict(title="On-Time %", side="right", overlaying="y"),
                    height=400,
                )
                st.plotly_chart(fig, use_container_width=True)

        st.subheader("Delivery Performance by State")
        df = query(Q.DELIVERY_BY_STATE)
        if not df.empty:
            st.dataframe(
                df.style.format({
                    "avg_delay_hours": "{:.1f}",
                    "on_time_pct": "{:.1f}%",
                }),
                use_container_width=True,
                hide_index=True,
            )

    # ── Reviews ──────────────────────────────────────────────────────────────
    with tab4:
        st.subheader("Customer Reviews & Satisfaction")
        col_l, col_r = st.columns(2)
        with col_l:
            df = query(Q.REVIEW_SCORE_DISTRIBUTION)
            if not df.empty:
                fig = px.bar(df, x="review_score", y="cnt",
                             color="review_score",
                             labels={"cnt": "Count", "review_score": "Score"},
                             color_continuous_scale="RdYlGn")
                fig.update_layout(height=350, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
        with col_r:
            df = query(Q.REVIEW_MONTHLY_TREND)
            if not df.empty:
                fig = px.line(df, x="month", y="avg_score",
                              labels={"avg_score": "Avg Score"}, markers=True)
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True)

        df = query(Q.REVIEW_RESPONSE_TIME)
        if not df.empty:
            st.subheader("Review Response Time")
            fig = px.pie(df, values="cnt", names="response_bucket", hole=0.3)
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    # ── Sellers ──────────────────────────────────────────────────────────────
    with tab5:
        st.subheader("Top Sellers by Revenue")
        df = query(Q.TOP_SELLERS_BY_REVENUE)
        if not df.empty:
            fig = px.bar(df, x="seller_id", y="revenue",
                         hover_data=["seller_city", "seller_state", "orders"],
                         labels={"revenue": "Revenue (R$)"})
            fig.update_layout(height=400, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Worst On-Time Delivery Sellers")
        df = query(Q.SELLER_DELIVERY_PERFORMANCE)
        if not df.empty:
            st.dataframe(
                df.style.format({
                    "avg_delay_hours": "{:.1f}",
                    "on_time_pct": "{:.1f}%",
                }),
                use_container_width=True,
                hide_index=True,
            )

    # ── Customer Segmentation ────────────────────────────────────────────────
    with tab6:
        st.subheader("Customer RFM Segmentation")
        df = query(Q.CUSTOMER_RFM)
        if not df.empty:
            col_l, col_r = st.columns(2)
            with col_l:
                fig = px.bar(df, x="segment", y="customers", color="segment",
                             labels={"customers": "Customers"})
                fig.update_layout(height=400, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
            with col_r:
                fig = px.scatter(df, x="avg_recency", y="avg_monetary",
                                 size="customers", color="segment",
                                 hover_data=["avg_frequency"],
                                 labels={"avg_recency": "Avg Recency (days)",
                                          "avg_monetary": "Avg Monetary (R$)"})
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)

        st.subheader("Repeat vs One-Time Customers")
        col_l, col_r = st.columns(2)
        with col_l:
            df = query(Q.CUSTOMER_REPEAT_VS_ONETIME)
            if not df.empty:
                fig = px.pie(df, values="customers", names="customer_type",
                             hole=0.4, title="By Count")
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)
        with col_r:
            df = query(Q.CUSTOMER_REPEAT_VS_ONETIME)
            if not df.empty:
                fig = px.pie(df, values="total_revenue", names="customer_type",
                             hole=0.4, title="By Revenue")
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)

        st.subheader("Customer Lifetime Value Distribution")
        df = query(Q.CUSTOMER_CLV_DISTRIBUTION)
        if not df.empty:
            fig = px.bar(df, x="clv_bucket", y="customers",
                         labels={"customers": "Customers", "clv_bucket": "CLV Range"})
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("New Customer Acquisition Trend")
        df = query(Q.CUSTOMER_ACQUISITION_TREND)
        if not df.empty:
            fig = px.area(df, x="cohort_month", y="new_customers",
                          labels={"new_customers": "New Customers", "cohort_month": "Month"})
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    # ── Product Performance ──────────────────────────────────────────────────
    with tab7:
        st.subheader("Top 20 Products by Revenue")
        df = query(Q.PRODUCT_TOP_BY_REVENUE)
        if not df.empty:
            fig = px.bar(df, x="product_id", y="revenue", color="category",
                         hover_data=["units_sold"],
                         labels={"revenue": "Revenue (R$)"})
            fig.update_layout(height=400, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Bottom 20 Products (min 5 sales)")
        df = query(Q.PRODUCT_BOTTOM_BY_REVENUE)
        if not df.empty:
            fig = px.bar(df, x="product_id", y="revenue", color="category",
                         hover_data=["units_sold"],
                         labels={"revenue": "Revenue (R$)"})
            fig.update_layout(height=400, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

        col_l, col_r = st.columns(2)
        with col_l:
            st.subheader("Price Distribution")
            df = query(Q.PRODUCT_PRICE_DISTRIBUTION)
            if not df.empty:
                fig = px.bar(df, x="price_bucket", y="items",
                             labels={"items": "Items Sold"})
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True)
        with col_r:
            st.subheader("Items Per Order")
            df = query(Q.ITEMS_PER_ORDER_DISTRIBUTION)
            if not df.empty:
                fig = px.bar(df, x="items_in_order", y="order_count",
                             labels={"order_count": "Orders", "items_in_order": "Items"})
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True)

        st.subheader("Category Revenue Heatmap (Monthly)")
        df = query(Q.PRODUCT_CATEGORY_HEATMAP)
        if not df.empty:
            pivot = df.pivot_table(index="category", columns="month",
                                   values="revenue", fill_value=0)
            top_cats = pivot.sum(axis=1).nlargest(15).index
            pivot = pivot.loc[top_cats]
            fig = px.imshow(pivot, aspect="auto",
                            labels=dict(x="Month", y="Category", color="Revenue"),
                            color_continuous_scale="YlOrRd")
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)

    # ── Geographic Analysis ──────────────────────────────────────────────────
    with tab8:
        st.subheader("Revenue by State")
        df = query(Q.REVENUE_BY_STATE)
        if not df.empty:
            col_l, col_r = st.columns(2)
            with col_l:
                fig = px.bar(df, x="state", y="revenue", color="state",
                             hover_data=["orders", "customers"],
                             labels={"revenue": "Revenue (R$)"})
                fig.update_layout(height=400, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
            with col_r:
                fig = px.treemap(df, path=["state"], values="revenue",
                                 color="orders", color_continuous_scale="Blues")
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)

        st.subheader("Top 20 Cities by Orders")
        df = query(Q.TOP_CITIES_BY_ORDERS)
        if not df.empty:
            fig = px.bar(df, x="city", y="orders", color="state",
                         hover_data=["revenue"],
                         labels={"orders": "Orders"})
            fig.update_layout(height=400, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Delivery Performance by Route (Seller→Customer State)")
        df = query(Q.DELIVERY_BY_ROUTE)
        if not df.empty:
            st.dataframe(
                df.style.format({
                    "avg_delay_hours": "{:.1f}",
                    "on_time_pct": "{:.1f}%",
                }),
                use_container_width=True,
                hide_index=True,
            )

        st.subheader("Regional Growth (Top 5 States)")
        df = query(Q.REGIONAL_GROWTH_TREND)
        if not df.empty:
            top_states = df.groupby("state")["orders"].sum().nlargest(5).index.tolist()
            df_top = df[df["state"].isin(top_states)]
            fig = px.line(df_top, x="month", y="orders", color="state",
                          labels={"orders": "Orders"}, markers=True)
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

    # ── Order Funnel / Lifecycle ─────────────────────────────────────────────
    with tab9:
        st.subheader("Order Status Breakdown")
        df = query(Q.ORDER_STATUS_BREAKDOWN)
        if not df.empty:
            col_l, col_r = st.columns(2)
            with col_l:
                fig = px.pie(df, values="orders", names="order_status", hole=0.3)
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True)
            with col_r:
                fig = px.bar(df, x="order_status", y="orders", color="order_status")
                fig.update_layout(height=350, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

        st.subheader("Average Time Between Lifecycle Stages")
        df = query(Q.ORDER_LIFECYCLE_TIMES)
        if not df.empty:
            stages = {
                "Approval": df["avg_approval_hours"].iloc[0],
                "Shipping": df["avg_ship_hours"].iloc[0],
                "Transit": df["avg_transit_hours"].iloc[0],
            }
            stages_df = pd.DataFrame({"stage": stages.keys(), "hours": stages.values()})
            stages_df = stages_df.dropna()
            fig = px.bar(stages_df, x="stage", y="hours",
                         labels={"hours": "Avg Hours"}, color="stage")
            fig.update_layout(height=350, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
            total_days = df['avg_total_days'].iloc[0]
            st.metric("Avg Total Order-to-Delivery",
                      f"{total_days:.1f} days" if total_days is not None else "N/A")

        st.subheader("Cancellation Rate Trend")
        df = query(Q.CANCELLATION_RATE_TREND)
        if not df.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(x=df["month"], y=df["cancelled"], name="Cancelled"))
            fig.add_trace(go.Scatter(x=df["month"], y=df["cancel_pct"],
                                     name="Cancel %", yaxis="y2", mode="lines+markers"))
            fig.update_layout(
                yaxis=dict(title="Cancelled Orders"),
                yaxis2=dict(title="Cancel %", side="right", overlaying="y"),
                height=400,
            )
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Order Lead Time Distribution (Order→Delivery)")
        df = query(Q.ORDER_LEAD_TIME_DISTRIBUTION)
        if not df.empty:
            fig = px.bar(df, x="lead_bucket", y="orders",
                         labels={"orders": "Orders", "lead_bucket": "Lead Time"})
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    # ── Revenue Cohort Analysis ──────────────────────────────────────────────
    with tab10:
        st.subheader("Customer Retention Cohorts")
        df = query(Q.CUSTOMER_COHORTS)
        if not df.empty:
            pivot = df.pivot_table(index="cohort_month", columns="month_offset",
                                   values="active_customers", fill_value=0)
            # Normalize to retention %
            retention = pivot.div(pivot[0], axis=0) * 100
            fig = px.imshow(retention, aspect="auto",
                            labels=dict(x="Months Since First Purchase",
                                        y="Cohort Month", color="Retention %"),
                            color_continuous_scale="YlGn")
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Cohort Revenue Over Time")
        df = query(Q.COHORT_REVENUE)
        if not df.empty:
            df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce").fillna(0)
            # Show top cohorts by size
            top_cohorts = df.groupby("cohort_month")["revenue"].sum().nlargest(8).index.tolist()
            df_top = df[df["cohort_month"].isin([str(c) for c in top_cohorts])]
            fig = px.line(df_top, x="month_offset", y="revenue",
                          color="cohort_month", markers=True,
                          labels={"month_offset": "Months Since First Purchase",
                                  "revenue": "Revenue (R$)"})
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

    # ── Seller Deep Dive ─────────────────────────────────────────────────────
    with tab11:
        st.subheader("Seller Revenue Concentration (Pareto)")
        df = query(Q.SELLER_CONCENTRATION)
        if not df.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(x=df["seller_rank"], y=df["revenue"],
                                 name="Revenue"))
            fig.add_trace(go.Scatter(x=df["seller_rank"], y=df["cumulative_pct"],
                                     name="Cumulative %", yaxis="y2",
                                     mode="lines", line=dict(color="red")))
            fig.update_layout(
                yaxis=dict(title="Revenue (R$)"),
                yaxis2=dict(title="Cumulative %", side="right", overlaying="y",
                            range=[0, 105]),
                height=400,
            )
            # Add 80% line
            fig.add_hline(y=80, line_dash="dash", line_color="gray",
                          annotation_text="80%", yref="y2")
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Seller Category Specialization")
        df = query(Q.SELLER_CATEGORY_SPECIALIZATION)
        if not df.empty:
            fig = px.scatter(df, x="total_revenue", y="specialization_pct",
                             color="primary_category", hover_data=["seller_id", "seller_state"],
                             labels={"total_revenue": "Total Revenue (R$)",
                                      "specialization_pct": "Specialization %"})
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Seller Rating vs Delivery Performance")
        df = query(Q.SELLER_RATING_VS_DELIVERY)
        if not df.empty:
            fig = px.scatter(df, x="avg_delay_hours", y="avg_rating",
                             size="deliveries", color="seller_state",
                             hover_data=["seller_id"],
                             labels={"avg_delay_hours": "Avg Delay (hours)",
                                      "avg_rating": "Avg Rating"})
            fig.add_vline(x=0, line_dash="dash", line_color="green",
                          annotation_text="On-Time")
            fig.update_layout(height=450)
            st.plotly_chart(fig, use_container_width=True)

    # ── Payment Installments ─────────────────────────────────────────────────
    with tab12:
        st.subheader("Installment Count Distribution")
        df = query(Q.INSTALLMENT_DISTRIBUTION)
        if not df.empty:
            col_l, col_r = st.columns(2)
            with col_l:
                fig = px.bar(df, x="payment_installments", y="txn_count",
                             labels={"txn_count": "Transactions",
                                      "payment_installments": "Installments"})
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True)
            with col_r:
                fig = px.bar(df, x="payment_installments", y="total_value",
                             labels={"total_value": "Total Value (R$)",
                                      "payment_installments": "Installments"})
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True)

        st.subheader("Avg Order Value by Installment Count")
        df = query(Q.INSTALLMENT_AOV)
        if not df.empty:
            fig = px.bar(df, x="payment_installments", y="avg_order_value",
                         text="orders",
                         labels={"avg_order_value": "Avg Order Value (R$)",
                                  "payment_installments": "Installments"})
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Installment Usage Trend")
        df = query(Q.INSTALLMENT_TREND)
        if not df.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(x=df["month"], y=df["avg_installments"],
                                 name="Avg Installments"))
            fig.add_trace(go.Scatter(x=df["month"], y=df["pct_installment_usage"],
                                     name="% Using Installments", yaxis="y2",
                                     mode="lines+markers"))
            fig.update_layout(
                yaxis=dict(title="Avg Installments"),
                yaxis2=dict(title="% Usage", side="right", overlaying="y"),
                height=400,
            )
            st.plotly_chart(fig, use_container_width=True)

    # ── Seasonality / Calendar ───────────────────────────────────────────────
    with tab13:
        st.subheader("Orders by Day of Week")
        df = query(Q.WEEKDAY_HOURLY_PATTERN)
        if not df.empty:
            fig = px.bar(df, x="day_name", y="orders", color="day_type",
                         hover_data=["avg_order_value"],
                         labels={"orders": "Orders"})
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Monthly Seasonality")
        df = query(Q.ORDERS_BY_MONTH_NAME)
        if not df.empty:
            col_l, col_r = st.columns(2)
            with col_l:
                fig = px.bar(df, x="month_name", y="orders",
                             labels={"orders": "Orders"})
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True)
            with col_r:
                fig = px.bar(df, x="month_name", y="revenue",
                             labels={"revenue": "Revenue (R$)"})
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True)

        st.subheader("Quarterly Performance (YoY)")
        df = query(Q.ORDERS_BY_QUARTER_YOY)
        if not df.empty:
            fig = px.bar(df, x="period", y="revenue", color="year",
                         text="orders", barmode="group",
                         labels={"revenue": "Revenue (R$)"})
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

    # ── Basket Analysis ──────────────────────────────────────────────────────
    with tab14:
        st.subheader("Average Basket Size & Value Trend")
        df = query(Q.BASKET_SIZE_TREND)
        if not df.empty:
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=df["month"], y=df["avg_basket_size"],
                                     name="Avg Items", mode="lines+markers"))
            fig.add_trace(go.Scatter(x=df["month"], y=df["avg_basket_value"],
                                     name="Avg Value (R$)", yaxis="y2",
                                     mode="lines+markers"))
            fig.update_layout(
                yaxis=dict(title="Avg Items per Order"),
                yaxis2=dict(title="Avg Value (R$)", side="right", overlaying="y"),
                height=400,
            )
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Multi-Item vs Single-Item Orders")
        df = query(Q.MULTI_ITEM_ORDERS_PCT)
        if not df.empty:
            col_l, col_r = st.columns(2)
            with col_l:
                fig = px.pie(df, values="orders", names="order_type", hole=0.4)
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)
            with col_r:
                fig = px.bar(df, x="order_type", y="avg_value",
                             labels={"avg_value": "Avg Order Value (R$)"},
                             color="order_type")
                fig.update_layout(height=300, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

        st.subheader("Top Category Combinations (Cross-sell)")
        df = query(Q.TOP_CATEGORY_COMBINATIONS)
        if not df.empty:
            df["pair"] = df["category_1"] + " + " + df["category_2"]
            fig = px.bar(df.head(15), x="pair", y="co_occurrence",
                         labels={"co_occurrence": "Co-occurrences", "pair": "Category Pair"})
            fig.update_layout(height=400, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

    # ── Freight & Logistics ──────────────────────────────────────────────────
    with tab15:
        st.subheader("Freight Cost Distribution")
        df = query(Q.FREIGHT_COST_DISTRIBUTION)
        if not df.empty:
            fig = px.bar(df, x="freight_bucket", y="items",
                         labels={"items": "Items", "freight_bucket": "Freight Cost"})
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Freight as % of Order Value")
        df = query(Q.FREIGHT_PCT_OF_ORDER)
        if not df.empty:
            fig = px.bar(df, x="freight_pct_bucket", y="orders",
                         labels={"orders": "Orders", "freight_pct_bucket": "Freight %"})
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Average Freight by Customer State")
        df = query(Q.FREIGHT_BY_STATE)
        if not df.empty:
            fig = px.bar(df, x="state", y="avg_freight", color="freight_pct",
                         hover_data=["total_freight", "avg_price"],
                         labels={"avg_freight": "Avg Freight (R$)",
                                  "freight_pct": "Freight %"},
                         color_continuous_scale="YlOrRd")
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Estimated vs Actual Delivery (Monthly)")
        df = query(Q.ESTIMATED_VS_ACTUAL_DELIVERY)
        if not df.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(x=df["month"], y=df["avg_diff_days"],
                                 name="Avg Diff (days)"))
            fig.add_trace(go.Scatter(x=df["month"], y=df["met_estimate_pct"],
                                     name="Met Estimate %", yaxis="y2",
                                     mode="lines+markers"))
            fig.update_layout(
                yaxis=dict(title="Avg Actual - Estimated (days)"),
                yaxis2=dict(title="Met Estimate %", side="right", overlaying="y"),
                height=400,
            )
            st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: ANALYTICS
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "🔬 Analytics":
    st.title("🔬 Analytics Deep Dive")

    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10, tab11, tab12, tab13, tab14, tab15 = st.tabs([
        "Category Analysis", "Geographic Insights", "Customer Behavior",
        "Calendar Intelligence", "Event & Payment Types",
        "Correlations", "Anomaly Detection", "Seller Churn & Growth",
        "Customer Journey", "Price Elasticity",
        "Delivery Accuracy", "Review Insights", "Revenue Decomposition",
        "Statistical Summary", "What-If Simulator"
    ])

    # ── Category Analysis ────────────────────────────────────────────────────
    with tab1:
        st.subheader("Top Categories by Revenue")
        df = query(Q.CATEGORY_REVENUE)
        if not df.empty:
            fig = px.bar(df, x="category", y="revenue",
                         hover_data=["orders", "freight"],
                         labels={"revenue": "Revenue (R$)"})
            fig.update_layout(xaxis_tickangle=-45, height=450)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Category Review Scores (min 10 reviews)")
        df = query(Q.CATEGORY_AVG_REVIEW)
        if not df.empty:
            fig = px.bar(df, x="category", y="avg_score",
                         color="avg_score", text="review_count",
                         color_continuous_scale="RdYlGn",
                         labels={"avg_score": "Avg Review Score"})
            fig.update_layout(xaxis_tickangle=-45, height=400)
            st.plotly_chart(fig, use_container_width=True)

    # ── Geographic Insights ──────────────────────────────────────────────────
    with tab2:
        st.subheader("Orders by State")
        df = query(Q.ORDERS_BY_STATE)
        if not df.empty:
            fig = px.bar(df, x="state", y="orders",
                         hover_data=["customers"],
                         labels={"orders": "Orders"})
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

            st.dataframe(df, use_container_width=True, hide_index=True)

    # ── Customer Behavior ────────────────────────────────────────────────────
    with tab3:
        st.subheader("Order Event Funnel")
        funnel_sql = """
        SELECT event_type, count(*) AS cnt
        FROM mart.fact_order_events
        GROUP BY event_type
        ORDER BY cnt DESC;
        """
        df = query(funnel_sql)
        if not df.empty:
            fig = px.funnel(df, x="cnt", y="event_type",
                            labels={"cnt": "Count", "event_type": "Event"})
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Daily Event Volume by Type")
        df = query(Q.EVENT_VOLUME_BY_DAY)
        if not df.empty:
            fig = px.area(df, x="event_date", y="event_count",
                          color="event_type",
                          labels={"event_count": "Events"})
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

    # ── Calendar Intelligence (dim_date) ─────────────────────────────────────
    with tab4:
        st.subheader("📅 Weekday vs Weekend Performance")
        col_l, col_r = st.columns(2)
        with col_l:
            df = query(Q.WEEKDAY_VS_WEEKEND)
            if not df.empty:
                fig = px.bar(df, x="day_type", y="orders",
                             color="day_type", text="revenue",
                             labels={"orders": "Orders"})
                fig.update_layout(height=300, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

        with col_r:
            df = query(Q.ORDERS_BY_DAY_OF_WEEK)
            if not df.empty:
                fig = px.bar(df, x="day_name", y="orders",
                             labels={"orders": "Orders", "day_name": "Day"})
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)

        st.subheader("Quarterly Performance (dim_date)")
        df = query(Q.QUARTERLY_PERFORMANCE)
        if not df.empty:
            df["period"] = df["year"].astype(str) + " Q" + df["quarter"].astype(str)
            fig = go.Figure()
            fig.add_trace(go.Bar(x=df["period"], y=df["revenue"], name="Revenue (R$)"))
            fig.add_trace(go.Scatter(x=df["period"], y=df["orders"],
                                     name="Orders", yaxis="y2", mode="lines+markers"))
            fig.update_layout(
                yaxis=dict(title="Revenue (R$)"),
                yaxis2=dict(title="Orders", side="right", overlaying="y"),
                height=400,
            )
            st.plotly_chart(fig, use_container_width=True)

    # ── Event & Payment Type Dimensions ──────────────────────────────────────
    with tab5:
        st.subheader("🏷️ Event Type Domain Map (dim_event_type)")
        df = query(Q.EVENT_TYPE_DOMAIN_SUMMARY)
        if not df.empty:
            fig = px.bar(df, x="event_type", y="event_count",
                         color="domain", text="is_terminal_state",
                         labels={"event_count": "Events"},
                         hover_data=["domain", "is_terminal_state"])
            fig.update_layout(xaxis_tickangle=-45, height=400)
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(df, use_container_width=True, hide_index=True)

        st.subheader("💳 Payment Type Analysis (dim_payment_type)")
        df = query(Q.PAYMENT_TYPE_ANALYSIS)
        if not df.empty:
            fig = px.bar(df, x="payment_type", y="total_value",
                         color="is_digital", text="txn_count",
                         hover_data=["is_installment_supported", "avg_installments"],
                         labels={"total_value": "Total Value (R$)"})
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(df, use_container_width=True, hide_index=True)

    # ── Correlation Analysis ─────────────────────────────────────────────────
    with tab6:
        st.subheader("Review Score vs Delivery Delay")
        df = query(Q.CORR_REVIEW_VS_DELAY)
        if not df.empty:
            fig = px.bar(df, x="delay_bucket", y="avg_score", text="reviews",
                         color="avg_score", color_continuous_scale="RdYlGn",
                         labels={"avg_score": "Avg Review Score"})
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

        col_l, col_r = st.columns(2)
        with col_l:
            st.subheader("Price vs Review Score")
            df = query(Q.CORR_PRICE_VS_REVIEW)
            if not df.empty:
                fig = px.bar(df, x="price_bucket", y="avg_score", text="reviews",
                             labels={"avg_score": "Avg Score"})
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True)
        with col_r:
            st.subheader("Freight vs Review Score")
            df = query(Q.CORR_FREIGHT_VS_REVIEW)
            if not df.empty:
                fig = px.bar(df, x="freight_bucket", y="avg_score", text="reviews",
                             labels={"avg_score": "Avg Score"})
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True)

        st.subheader("Payment Method vs Review Score")
        df = query(Q.CORR_PAYMENT_METHOD_VS_REVIEW)
        if not df.empty:
            fig = px.bar(df, x="payment_type", y="avg_score", text="reviews",
                         color="avg_score", color_continuous_scale="RdYlGn",
                         labels={"avg_score": "Avg Score"})
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    # ── Anomaly Detection ────────────────────────────────────────────────────
    with tab7:
        st.subheader("Daily Order Volume — Anomalies (|Z| > 2)")
        df = query(Q.ANOMALY_DAILY_ORDERS)
        if not df.empty:
            df["is_anomaly"] = df["z_score"].abs() > 2
            fig = go.Figure()
            normal = df[~df["is_anomaly"]]
            anomaly = df[df["is_anomaly"]]
            fig.add_trace(go.Scatter(x=normal["event_date"], y=normal["orders"],
                                     mode="lines", name="Normal", line=dict(color="steelblue")))
            fig.add_trace(go.Scatter(x=anomaly["event_date"], y=anomaly["orders"],
                                     mode="markers", name="Anomaly",
                                     marker=dict(color="red", size=8)))
            fig.add_hline(y=df["mean_orders"].iloc[0] + 2*df["std_orders"].iloc[0],
                          line_dash="dash", line_color="orange", annotation_text="+2σ")
            fig.add_hline(y=df["mean_orders"].iloc[0] - 2*df["std_orders"].iloc[0],
                          line_dash="dash", line_color="orange", annotation_text="-2σ")
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Daily Revenue — Anomalies (|Z| > 2)")
        df = query(Q.ANOMALY_DAILY_REVENUE)
        if not df.empty:
            df["is_anomaly"] = df["z_score"].abs() > 2
            fig = go.Figure()
            normal = df[~df["is_anomaly"]]
            anomaly = df[df["is_anomaly"]]
            fig.add_trace(go.Scatter(x=normal["event_date"], y=normal["revenue"],
                                     mode="lines", name="Normal", line=dict(color="green")))
            fig.add_trace(go.Scatter(x=anomaly["event_date"], y=anomaly["revenue"],
                                     mode="markers", name="Anomaly",
                                     marker=dict(color="red", size=8)))
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Monthly Delivery Delay Anomalies")
        df = query(Q.ANOMALY_DELIVERY_DELAY)
        if not df.empty:
            df["is_anomaly"] = df["z_score"].abs() > 2
            fig = px.bar(df, x="month", y="avg_delay", color="is_anomaly",
                         color_discrete_map={True: "red", False: "steelblue"},
                         labels={"avg_delay": "Avg Delay (h)"})
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Category Revenue Surges (|Z| > 2)")
        df = query(Q.ANOMALY_CATEGORY_SURGE)
        if not df.empty:
            st.dataframe(
                df.style.format({
                    "revenue": "R${:,.0f}",
                    "mean_rev": "R${:,.0f}",
                    "z_score": "{:.2f}",
                }),
                use_container_width=True,
                hide_index=True,
            )

    # ── Seller Churn & Growth ────────────────────────────────────────────────
    with tab8:
        st.subheader("Seller Onboarding Trend")
        df = query(Q.SELLER_ONBOARDING_TREND)
        if not df.empty:
            fig = px.bar(df, x="onboard_month", y="new_sellers",
                         labels={"new_sellers": "New Sellers", "onboard_month": "Month"})
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Seller Activity Status")
        df = query(Q.SELLER_INACTIVE)
        if not df.empty:
            col_l, col_r = st.columns(2)
            with col_l:
                fig = px.pie(df, values="sellers", names="status", hole=0.4)
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True)
            with col_r:
                fig = px.bar(df, x="status", y="sellers", color="status")
                fig.update_layout(height=350, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

        col_l, col_r = st.columns(2)
        with col_l:
            st.subheader("Top 20 Growing Sellers (MoM)")
            df = query(Q.SELLER_REVENUE_GROWTH)
            if not df.empty:
                st.dataframe(
                    df.style.format({
                        "current_revenue": "R${:,.0f}",
                        "prev_revenue": "R${:,.0f}",
                        "growth_pct": "{:+.1f}%",
                    }),
                    use_container_width=True, hide_index=True,
                )
        with col_r:
            st.subheader("Top 20 Declining Sellers (MoM)")
            df = query(Q.SELLER_REVENUE_DECLINE)
            if not df.empty:
                st.dataframe(
                    df.style.format({
                        "current_revenue": "R${:,.0f}",
                        "prev_revenue": "R${:,.0f}",
                        "growth_pct": "{:+.1f}%",
                    }),
                    use_container_width=True, hide_index=True,
                )

    # ── Customer Journey Analysis ────────────────────────────────────────────
    with tab9:
        st.subheader("Time to Second Purchase")
        df = query(Q.CUSTOMER_TIME_TO_SECOND_PURCHASE)
        if not df.empty:
            fig = px.bar(df, x="bucket", y="customers",
                         labels={"customers": "Customers", "bucket": "Days to 2nd Purchase"})
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Purchase Frequency Distribution")
        df = query(Q.CUSTOMER_PURCHASE_FREQUENCY)
        if not df.empty:
            col_l, col_r = st.columns(2)
            with col_l:
                fig = px.bar(df, x="frequency_bucket", y="customers",
                             labels={"customers": "Customers"})
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True)
            with col_r:
                fig = px.bar(df, x="frequency_bucket", y="avg_spent",
                             labels={"avg_spent": "Avg Total Spent (R$)"})
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True)

        st.subheader("Customer Dormancy Analysis")
        df = query(Q.CUSTOMER_DORMANCY)
        if not df.empty:
            fig = px.pie(df, values="customers", names="status", hole=0.4,
                         color_discrete_sequence=px.colors.sequential.RdBu)
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

    # ── Price Elasticity ─────────────────────────────────────────────────────
    with tab10:
        st.subheader("Price vs Volume by Category")
        df = query(Q.PRICE_VS_VOLUME_BY_CATEGORY)
        if not df.empty:
            df["total_revenue"] = pd.to_numeric(df["total_revenue"], errors="coerce").fillna(0)
            fig = px.scatter(df, x="avg_price", y="units_sold",
                             size="total_revenue", color="category",
                             hover_data=["category"],
                             labels={"avg_price": "Avg Price (R$)",
                                      "units_sold": "Units Sold"})
            fig.update_layout(height=450, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Installment Group Impact on Order Size")
        df = query(Q.HIGH_INSTALLMENT_IMPACT)
        if not df.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(x=df["installment_group"], y=df["avg_payment"],
                                 name="Avg Payment (R$)"))
            fig.add_trace(go.Scatter(x=df["installment_group"], y=df["orders"],
                                     name="Orders", yaxis="y2", mode="lines+markers"))
            fig.update_layout(
                yaxis=dict(title="Avg Payment (R$)"),
                yaxis2=dict(title="Orders", side="right", overlaying="y"),
                height=400,
            )
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Category Price Sensitivity (Price-Volume Correlation)")
        df = query(Q.CATEGORY_PRICE_SENSITIVITY)
        if not df.empty:
            fig = px.bar(df, x="category", y="price_volume_corr",
                         color="price_volume_corr",
                         color_continuous_scale="RdYlGn",
                         hover_data=["mean_price", "mean_volume", "months"],
                         labels={"price_volume_corr": "Correlation"})
            fig.update_layout(height=450, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Negative correlation = higher price → lower volume (price sensitive)")

    # ── Delivery Prediction Accuracy ─────────────────────────────────────────
    with tab11:
        st.subheader("Estimated vs Actual Delivery Days (sample)")
        df = query(Q.DELIVERY_ESTIMATION_SCATTER)
        if not df.empty:
            fig = px.scatter(df, x="estimated_days", y="actual_days",
                             opacity=0.3,
                             labels={"estimated_days": "Estimated Days",
                                      "actual_days": "Actual Days"})
            # Perfect prediction line
            max_val = max(df["estimated_days"].max(), df["actual_days"].max())
            fig.add_trace(go.Scatter(x=[0, max_val], y=[0, max_val],
                                     mode="lines", name="Perfect",
                                     line=dict(dash="dash", color="red")))
            fig.update_layout(height=450)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Estimation Accuracy by Seller State")
        df = query(Q.DELIVERY_ESTIMATION_BY_SELLER_STATE)
        if not df.empty:
            fig = px.bar(df, x="seller_state", y="avg_diff_days",
                         color="met_estimate_pct",
                         color_continuous_scale="RdYlGn",
                         hover_data=["deliveries", "met_estimate_pct"],
                         labels={"avg_diff_days": "Avg (Actual - Estimated) days"})
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Over-Promise vs Under-Promise Distribution")
        df = query(Q.DELIVERY_OVER_UNDER_PROMISE)
        if not df.empty:
            fig = px.bar(df, x="promise_bucket", y="deliveries",
                         labels={"deliveries": "Deliveries"})
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Over-promised = delivered later than estimated; Under-promised = delivered earlier")

    # ── Review Insights ──────────────────────────────────────────────────────
    with tab12:
        st.subheader("Low-Score Reviews by Category (% scoring ≤2)")
        df = query(Q.REVIEW_BY_CATEGORY)
        if not df.empty:
            fig = px.bar(df, x="category", y="low_score_pct",
                         color="avg_score", color_continuous_scale="RdYlGn",
                         hover_data=["reviews", "low_score_count"],
                         labels={"low_score_pct": "% Low Score (≤2)"})
            fig.update_layout(height=400, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Likely Causes of Low Reviews (score ≤ 2)")
        df = query(Q.REVIEW_LOW_SCORE_CAUSES)
        if not df.empty:
            fig = px.pie(df, values="low_reviews", names="likely_cause", hole=0.3)
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Delivery Performance Tier vs Avg Review")
        df = query(Q.REVIEW_BY_SELLER_PERFORMANCE)
        if not df.empty:
            fig = px.bar(df, x="delivery_tier", y="avg_review_score",
                         text="sellers", color="avg_review_score",
                         color_continuous_scale="RdYlGn",
                         labels={"avg_review_score": "Avg Review Score"})
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    # ── Revenue Decomposition ────────────────────────────────────────────────
    with tab13:
        st.subheader("Monthly Revenue Growth Drivers")
        df = query(Q.REVENUE_GROWTH_DRIVERS)
        if not df.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(x=df["month"], y=df["revenue"], name="Revenue (R$)"))
            fig.add_trace(go.Scatter(x=df["month"], y=df["orders_per_customer"],
                                     name="Orders/Customer", yaxis="y2", mode="lines+markers"))
            fig.add_trace(go.Scatter(x=df["month"], y=df["avg_order_value"],
                                     name="AOV (R$)", yaxis="y2", mode="lines+markers",
                                     line=dict(dash="dot")))
            fig.update_layout(
                yaxis=dict(title="Revenue (R$)"),
                yaxis2=dict(title="Ratio / AOV", side="right", overlaying="y"),
                height=450, legend=dict(x=0, y=1.15, orientation="h"),
            )
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Revenue: New vs Repeat Customers")
        df = query(Q.REVENUE_NEW_VS_REPEAT)
        if not df.empty:
            fig = px.area(df, x="month", y="revenue", color="customer_type",
                          labels={"revenue": "Revenue (R$)"})
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("MoM Revenue Change (Waterfall)")
        df = query(Q.REVENUE_WATERFALL)
        if not df.empty and len(df) > 1:
            df = df.dropna(subset=["prev_revenue"])
            fig = go.Figure(go.Waterfall(
                x=df["month"].astype(str),
                y=df["change"],
                measure=["relative"] * len(df),
                text=[f"R${v:+,.0f}" for v in df["change"]],
                connector=dict(line=dict(color="gray")),
            ))
            fig.update_layout(height=400, title="Monthly Revenue Change")
            st.plotly_chart(fig, use_container_width=True)

    # ── Statistical Summary ──────────────────────────────────────────────────
    with tab14:
        st.subheader("Descriptive Statistics")
        stats_queries = [
            ("Price", Q.STATS_PRICE),
            ("Freight", Q.STATS_FREIGHT),
            ("Delivery Delay", Q.STATS_DELIVERY_DELAY),
            ("Review Score", Q.STATS_REVIEW),
            ("Payment Value", Q.STATS_PAYMENT_VALUE),
        ]
        frames = []
        for name, sql in stats_queries:
            df = query(sql)
            if not df.empty:
                frames.append(df)
        if frames:
            stats_df = pd.concat(frames, ignore_index=True)
            st.dataframe(
                stats_df.style.format({
                    "n": "{:,.0f}",
                    "mean": "{:.2f}",
                    "median": "{:.2f}",
                    "stddev": "{:.2f}",
                    "min_val": "{:.2f}",
                    "q1": "{:.2f}",
                    "q3": "{:.2f}",
                    "max_val": "{:.2f}",
                }),
                use_container_width=True,
                hide_index=True,
            )

        st.subheader("Box Plot Comparison")
        if frames:
            # Show a visual comparison of distributions
            box_data = []
            for _, sql in stats_queries:
                df = query(sql)
                if not df.empty:
                    row = df.iloc[0]
                    box_data.append({
                        "metric": row["metric"],
                        "min": row["min_val"],
                        "q1": row["q1"],
                        "median": row["median"],
                        "q3": row["q3"],
                        "max": row["max_val"],
                    })
            if box_data:
                box_df = pd.DataFrame(box_data)
                st.dataframe(box_df, use_container_width=True, hide_index=True)
                st.caption("IQR = Q3 - Q1. Outliers lie beyond 1.5×IQR from Q1/Q3.")

    # ── What-If Simulator ────────────────────────────────────────────────────
    with tab15:
        st.subheader("📊 Current Baseline Metrics")
        df = query(Q.WHATIF_BASELINE)
        if not df.empty:
            row = df.iloc[0]
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Revenue", f"R${row['total_revenue']:,.0f}")
            c2.metric("Avg Order Value", f"R${row['avg_order_value']:,.0f}")
            c3.metric("On-Time Delivery", f"{row['on_time_pct']:.1f}%")
            c4.metric("Repeat Rate", f"{row['repeat_rate_pct']:.1f}%")

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Orders", f"{row['total_orders']:,}")
            c2.metric("Avg Review", f"{row['avg_review']:.2f}")
            c3.metric("Avg Freight", f"R${row['avg_freight']:.2f}")
            c4.metric("—", "—")

        st.divider()
        st.subheader("🔮 What-If Scenarios")

        col_l, col_r = st.columns(2)
        with col_l:
            sla_improvement = st.slider("Improve On-Time Delivery by (%pts)", 0, 30, 10)
            freight_reduction = st.slider("Reduce Avg Freight by (%)", 0, 50, 15)
        with col_r:
            repeat_improvement = st.slider("Improve Repeat Rate by (%pts)", 0, 20, 5)
            aov_increase = st.slider("Increase AOV by (%)", 0, 30, 10)

        if not df.empty:
            row = df.iloc[0]
            # Convert Decimal types to float for arithmetic
            total_revenue = float(row["total_revenue"] or 0)
            avg_freight = float(row["avg_freight"] or 0)
            total_orders = float(row["total_orders"] or 0)
            on_time_pct = float(row["on_time_pct"] or 0)
            # Calculate projected impact
            new_on_time = min(100, on_time_pct + sla_improvement)
            # Better delivery → better reviews → less churn → ~0.5% revenue per %pt SLA improvement
            sla_revenue_boost = total_revenue * (sla_improvement * 0.005)
            freight_savings = avg_freight * (freight_reduction / 100) * total_orders
            repeat_revenue = total_revenue * (repeat_improvement / 100) * 0.3
            aov_revenue = total_revenue * (aov_increase / 100)

            total_impact = sla_revenue_boost + freight_savings + repeat_revenue + aov_revenue

            st.subheader("Projected Impact")
            c1, c2 = st.columns(2)
            with c1:
                st.metric("SLA Improvement Revenue", f"+R${sla_revenue_boost:,.0f}")
                st.metric("Freight Savings (customer)", f"+R${freight_savings:,.0f}")
            with c2:
                st.metric("Repeat Rate Revenue", f"+R${repeat_revenue:,.0f}")
                st.metric("AOV Increase Revenue", f"+R${aov_revenue:,.0f}")

            st.metric("**Total Projected Revenue Impact**",
                      f"+R${total_impact:,.0f}",
                      delta=f"+{100*total_impact/total_revenue:.1f}%" if total_revenue > 0 else "N/A")

        st.divider()
        st.subheader("⚠️ Revenue at Risk from Late Deliveries")
        df_risk = query(Q.WHATIF_LATE_DELIVERY_REVENUE_LOSS)
        if not df_risk.empty:
            row = df_risk.iloc[0]
            c1, c2 = st.columns(2)
            c1.metric("Orders with Late Delivery + Low Review", f"{row['late_low_review_orders']:,}")
            c2.metric("Revenue at Risk", f"R${row['revenue_at_risk']:,.0f}")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: OBSERVABILITY
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "🔭 Observability":
    st.title("🔭 Pipeline Observability & Monitoring")

    tab1, tab2, tab3, tab4 = st.tabs(["Ingestion Health", "Data Quality", "Data Completeness", "Alerts"])

    # ── Ingestion Health ─────────────────────────────────────────────────────
    with tab1:
        st.subheader("Table Freshness & Row Counts")
        df = query(Q.INGESTION_FRESHNESS)
        if not df.empty:
            # Color-code lag
            warn_sec = DASH_CFG.get("ingestion_lag_warn_sec", 120)
            crit_sec = DASH_CFG.get("ingestion_lag_critical_sec", 300)

            def lag_status(lag):
                if lag is None:
                    return "⚪ No data"
                if lag <= warn_sec:
                    return "🟢 Fresh"
                elif lag <= crit_sec:
                    return "🟡 Warning"
                else:
                    return "🔴 Stale"

            df["status"] = df["lag_seconds"].apply(lag_status)
            df["lag_display"] = df["lag_seconds"].apply(
                lambda x: f"{x/3600:.1f}h" if x and x > 3600
                else (f"{x/60:.0f}m" if x and x > 60 else f"{x:.0f}s" if x else "N/A")
            )
            st.dataframe(
                df[["table_name", "last_ingestion", "lag_display", "total_rows", "status"]],
                use_container_width=True,
                hide_index=True,
            )

        st.subheader("Ingestion Rate (hourly)")
        df = query(Q.INGESTION_RATE_HOURLY)
        if not df.empty:
            fig = px.bar(df, x="hour", y="events_ingested",
                         labels={"events_ingested": "Events", "hour": "Hour"})
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Table Row Counts")
        df = query(Q.TABLE_ROW_COUNTS)
        if not df.empty:
            fig = px.bar(df, x="table_name", y="row_count",
                         color="row_count", color_continuous_scale="Blues",
                         labels={"row_count": "Rows"})
            fig.update_layout(xaxis_tickangle=-45, height=350)
            st.plotly_chart(fig, use_container_width=True)

    # ── Data Quality ─────────────────────────────────────────────────────────
    with tab2:
        st.subheader("Data Quality Summary by Dataset")
        df = query(Q.DATA_QUALITY_SUMMARY)
        if not df.empty:
            st.dataframe(df, use_container_width=True, hide_index=True)

            # Violations chart
            viol_cols = ["total_null_violations", "total_duplicate_violations", "total_schema_violations"]
            available = [c for c in viol_cols if c in df.columns and df[c].sum() > 0]
            if available:
                fig = px.bar(df, x="dataset_name", y=available, barmode="group",
                             labels={"value": "Violations", "variable": "Type"})
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data quality runs recorded yet.")

        st.subheader("Recent Quality Runs (last 50)")
        df = query(Q.DATA_QUALITY_RUNS)
        if not df.empty:
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No quality run history.")

    # ── Data Completeness ────────────────────────────────────────────────────
    with tab4:
        st.subheader("🔍 Data Completeness (Null Analysis)")
        df = query(Q.DATA_COMPLETENESS)
        if not df.empty:
            st.dataframe(df, use_container_width=True, hide_index=True)

        st.subheader("🔁 Duplicate Check")
        df = query(Q.DUPLICATE_CHECK)
        if not df.empty:
            st.dataframe(df, use_container_width=True, hide_index=True)
            total_dupes = df["duplicates"].sum() if "duplicates" in df.columns else 0
            if total_dupes == 0:
                st.success("✅ No duplicates found across all fact tables.")
            else:
                st.warning(f"⚠️ {total_dupes} duplicate event_ids detected.")

        st.subheader("📈 Pipeline Throughput (daily)")
        df = query(Q.PIPELINE_THROUGHPUT)
        if not df.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(x=df["day"], y=df["events_ingested"],
                                 name="Events Ingested"))
            fig.add_trace(go.Scatter(x=df["day"], y=df["avg_lag_seconds"],
                                     name="Avg Lag (s)", yaxis="y2", mode="lines+markers"))
            fig.update_layout(
                yaxis=dict(title="Events"),
                yaxis2=dict(title="Avg Lag (seconds)", side="right", overlaying="y"),
                height=400,
            )
            st.plotly_chart(fig, use_container_width=True)

    # ── Alerts ───────────────────────────────────────────────────────────────
    with tab3:
        st.subheader("⚠️ Active Alerts")

        alerts = []

        # Alert: Ingestion lag
        df = query(Q.INGESTION_FRESHNESS)
        if not df.empty:
            crit_sec = DASH_CFG.get("ingestion_lag_critical_sec", 300)
            stale = df[df["lag_seconds"].notna() & (df["lag_seconds"] > crit_sec)]
            for _, row in stale.iterrows():
                alerts.append({
                    "severity": "🔴 CRITICAL",
                    "type": "Stale Data",
                    "detail": f"{row['table_name']} last ingested {row['lag_seconds']/3600:.1f}h ago",
                })

        # Alert: Data quality failures
        dq = query(Q.DATA_QUALITY_SUMMARY)
        if not dq.empty:
            failed = dq[dq["failed_runs"] > 0]
            for _, row in failed.iterrows():
                alerts.append({
                    "severity": "🟡 WARNING",
                    "type": "Quality Failure",
                    "detail": f"{row['dataset_name']}: {row['failed_runs']} failed runs",
                })

        # Alert: Low review scores
        rev = query(Q.KPI_AVG_REVIEW_SCORE)
        if not rev.empty and rev.iloc[0, 0] < 3.5:
            alerts.append({
                "severity": "🟡 WARNING",
                "type": "Low Satisfaction",
                "detail": f"Average review score: {rev.iloc[0, 0]:.2f} (below 3.5 threshold)",
            })

        # Alert: On-time delivery
        otd = query(Q.KPI_DELIVERY_ON_TIME_PCT)
        if not otd.empty and otd.iloc[0, 0] < 85:
            alerts.append({
                "severity": "🟡 WARNING",
                "type": "Delivery SLA",
                "detail": f"On-time delivery: {otd.iloc[0, 0]:.1f}% (below 85% target)",
            })

        if alerts:
            alert_df = pd.DataFrame(alerts)
            st.dataframe(alert_df, use_container_width=True, hide_index=True)
        else:
            st.success("✅ No active alerts. All systems healthy.")

        st.markdown("---")
        st.subheader("Alert Thresholds")
        st.json({
            "ingestion_lag_warn_sec": DASH_CFG.get("ingestion_lag_warn_sec"),
            "ingestion_lag_critical_sec": DASH_CFG.get("ingestion_lag_critical_sec"),
            "delivery_sla_target_pct": 85,
            "review_score_threshold": 3.5,
        })
