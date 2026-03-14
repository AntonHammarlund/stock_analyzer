import streamlit as st
import pandas as pd
import plotly.express as px

from stock_analyzer.pipeline import run_daily
from stock_analyzer.reports import load_latest_report
from stock_analyzer.portfolio import load_portfolio, save_portfolio, portfolio_summary

st.set_page_config(page_title="Stock Analyzer", layout="wide")

CALM_COLORS = ["#4f6b5e", "#8aa39b", "#c5bfb7", "#d8c9b3", "#b98c6b"]
px.defaults.template = "simple_white"
px.defaults.color_discrete_sequence = CALM_COLORS

st.markdown(
    """
    <style>
        :root {
            --sand: #f4f1ec;
            --linen: #fbfaf7;
            --ink: #2f2a26;
            --muted: #6b635c;
        }
        .main { background-color: var(--linen); color: var(--ink); }
        .block-container { padding-top: 1.5rem; padding-bottom: 2.5rem; }
        section[data-testid="stSidebar"] { background-color: var(--sand); }
        div[data-testid="stMetric"] {
            background: #ffffff;
            border: 1px solid #e7e1d8;
            padding: 0.9rem 1rem;
            border-radius: 12px;
        }
        div[data-testid="stMetricLabel"] { color: var(--muted); }
        hr { border-color: #e7e1d8; }
    </style>
    """,
    unsafe_allow_html=True,
)


def format_timestamp(value: str) -> str:
    if not value:
        return "Not yet generated"
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        return value
    return parsed.strftime("%Y-%m-%d %H:%M UTC")


def format_metric(value, fmt="{:.2f}") -> str:
    if value is None:
        return "n/a"
    try:
        return fmt.format(float(value))
    except (TypeError, ValueError):
        return str(value)


st.title("Stock Analyzer")
st.caption("Daily signal scan with portfolio context. Calm, quick, and decision-ready.")

with st.sidebar:
    st.header("Controls")
    run_now = st.button("Run Daily Refresh", use_container_width=True)
    st.caption("Fetches latest prices and recomputes scores.")

if run_now:
    report = run_daily()
    st.sidebar.success("Daily refresh completed.")
else:
    report = load_latest_report()

report = report or {}
generated_at = report.get("generated_at")

top_picks = report.get("top_picks", [])
outlook = report.get("outlook", {})
notes = report.get("notes", [])

holdings = load_portfolio()
portfolio_snapshot = portfolio_summary(holdings)

st.sidebar.divider()
st.sidebar.header("Status")
st.sidebar.caption(f"Last report: {format_timestamp(generated_at)}")
st.sidebar.caption(f"Top picks: {len(top_picks)}")
st.sidebar.caption(f"Holdings: {len(holdings)}")

if not report:
    st.info("No report found yet. Run Daily Refresh to generate the first report.")

tab_overview, tab_top, tab_outlook, tab_portfolio = st.tabs(
    ["Overview", "Top Picks", "Market Outlook", "Portfolio"]
)

with tab_overview:
    st.subheader("Overview")
    top_df = pd.DataFrame(top_picks)

    best_score = None
    avg_confidence = None
    top_pick_name = "n/a"
    if not top_df.empty:
        top_df = top_df.sort_values("score", ascending=False)
        best_score = top_df["score"].max()
        avg_confidence = top_df["confidence"].mean()
        top_pick_name = top_df.iloc[0].get("name", "n/a")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Top Picks", len(top_df))
    col2.metric("Best Score", format_metric(best_score))
    col3.metric("Avg Confidence", format_metric(avg_confidence))
    col4.metric("Concentration (HHI)", format_metric(portfolio_snapshot.get("concentration")))

    st.caption(f"Last report generated: {format_timestamp(generated_at)}")

    st.markdown("**Portfolio Lens**")
    st.write(portfolio_snapshot.get("risk", "No portfolio summary yet."))
    st.caption(portfolio_snapshot.get("positives", ""))

    if top_df.empty:
        st.info("Generate a report to populate top picks and market context.")
    else:
        st.markdown("**Current Leader**")
        st.write(f"{top_pick_name} leads with a score of {format_metric(best_score)}.")

    if notes:
        st.markdown("**Notes**")
        for note in notes:
            st.write(f"- {note}")

with tab_top:
    st.subheader("Top Picks")
    top_df = pd.DataFrame(top_picks)
    if top_df.empty:
        st.warning("No top picks available yet.")
    else:
        top_df = top_df.sort_values("score", ascending=False)

        with st.expander("Filters", expanded=False):
            asset_options = sorted(top_df.get("asset_type", pd.Series(dtype=str)).dropna().unique().tolist())
            selected_assets = asset_options
            if asset_options:
                selected_assets = st.multiselect("Asset types", asset_options, default=asset_options)
            min_conf = st.slider("Minimum confidence", 0.0, 1.0, value=0.0, step=0.05)

        filtered = top_df.copy()
        if selected_assets:
            filtered = filtered[filtered["asset_type"].isin(selected_assets)]
        if "confidence" in filtered.columns:
            filtered = filtered[filtered["confidence"] >= min_conf]

        st.caption(f"Showing {len(filtered)} of {len(top_df)} picks.")

        if filtered.empty:
            st.info("No picks match the current filters.")
        else:
            fig = px.bar(
                filtered,
                x="score",
                y="name",
                color="asset_type",
                orientation="h",
                hover_data=["confidence", "risk_score", "horizon"],
            )
            fig.update_layout(xaxis_title="Score", yaxis_title="", height=420)
            st.plotly_chart(fig, use_container_width=True)

            display_cols = [
                col
                for col in [
                    "instrument_id",
                    "name",
                    "asset_type",
                    "score",
                    "risk_score",
                    "confidence",
                    "horizon",
                ]
                if col in filtered.columns
            ]
            st.dataframe(filtered[display_cols], use_container_width=True, hide_index=True)

            if "rationale" in filtered.columns:
                with st.expander("Rationales", expanded=False):
                    st.dataframe(filtered[["name", "rationale"]], use_container_width=True, hide_index=True)

with tab_outlook:
    st.subheader("Market Outlook")
    daily = outlook.get("daily", "No daily summary yet.")
    deep = outlook.get("deep", "No deep summary yet.")

    st.markdown("**Daily Pulse**")
    st.write(daily)
    st.divider()
    st.markdown("**Deeper View**")
    st.write(deep)

with tab_portfolio:
    st.subheader("Portfolio")

    holdings_df = pd.DataFrame(holdings)
    if not holdings_df.empty:
        holdings_df["weight"] = pd.to_numeric(
            holdings_df.get("weight", 0), errors="coerce"
        ).fillna(0.0)
        total_weight = holdings_df["weight"].sum()
        if total_weight > 0:
            holdings_df["weight_norm"] = holdings_df["weight"] / total_weight
        else:
            holdings_df["weight_norm"] = 1 / len(holdings_df)
    else:
        total_weight = 0.0

    max_weight = None
    if not holdings_df.empty:
        max_weight = holdings_df["weight_norm"].max()

    col1, col2, col3 = st.columns(3)
    col1.metric("Holdings", len(holdings))
    col2.metric("Concentration (HHI)", format_metric(portfolio_snapshot.get("concentration")))
    col3.metric("Largest Weight", format_metric(max_weight))

    st.write(portfolio_snapshot.get("risk", "No holdings added yet."))
    st.caption(portfolio_snapshot.get("positives", ""))

    if holdings_df.empty:
        st.info("No holdings added yet.")
    else:
        fig = px.pie(
            holdings_df,
            names="name",
            values="weight_norm",
            hole=0.5,
            title="Weight Mix",
        )
        fig.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(
            holdings_df[["instrument_id", "name", "weight"]],
            use_container_width=True,
            hide_index=True,
        )

    st.markdown("### Add Holding")
    with st.form("add_holding"):
        instrument_id = st.text_input("Instrument ID")
        name = st.text_input("Name")
        weight = st.number_input(
            "Weight", min_value=0.0, max_value=1.0, value=0.1, step=0.01
        )
        submitted = st.form_submit_button("Add")
        if submitted:
            if not instrument_id or not name:
                st.warning("Instrument ID and name are required.")
            else:
                holding = {
                    "instrument_id": instrument_id.strip(),
                    "name": name.strip(),
                    "weight": float(weight),
                }
                holdings.append(holding)
                save_portfolio(holdings)
                st.success("Holding added.")
                st.rerun()

    if holdings:
        st.markdown("### Remove Holding")
        options = [
            f"{h.get('instrument_id')} - {h.get('name')}" for h in holdings
        ]
        to_remove = st.selectbox("Select holding", options)
        if st.button("Remove"):
            index = options.index(to_remove)
            holdings.pop(index)
            save_portfolio(holdings)
            st.success("Holding removed.")
            st.rerun()
