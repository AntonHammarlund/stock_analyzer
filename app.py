import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from stock_analyzer.pipeline import run_daily
from stock_analyzer.reports import load_latest_report
from stock_analyzer.portfolio import load_portfolio, save_portfolio, portfolio_summary
from stock_analyzer.users import (
    add_user,
    get_active_user_id,
    list_users,
    set_active_user_id,
)
from stock_analyzer.universe import build_universe
from stock_analyzer.data_sources.price_data import load_prices
from stock_analyzer.data_sources.universe_import import ImportedUniverseSource

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


@st.cache_data(ttl=3600)
def load_universe() -> pd.DataFrame:
    df = build_universe()
    if df.empty:
        return df
    df = df.copy()
    df["asset_type"] = df.get("asset_type", "").fillna("").astype(str).str.lower()
    return df


@st.cache_data(ttl=3600)
def load_imported_universe() -> pd.DataFrame:
    df = ImportedUniverseSource().fetch()
    if df.empty:
        return df
    df = df.copy()
    df["asset_type"] = df.get("asset_type", "").fillna("").astype(str).str.lower()
    return df


st.title("Stock Analyzer")
st.caption("Daily signal scan with portfolio context. Calm, quick, and decision-ready.")

with st.sidebar:
    st.header("Controls")
    run_now = st.button("Run Daily Refresh", use_container_width=True)
    st.caption("Fetches latest prices and recomputes scores.")

    st.divider()
    st.subheader("Account")
    users = list_users()
    active_user_id = get_active_user_id()
    user_names = [user.get("name", "User") for user in users]
    active_index = 0
    for idx, user in enumerate(users):
        if user.get("id") == active_user_id:
            active_index = idx
            break

    if user_names:
        selected_name = st.selectbox("Active user", user_names, index=active_index)
        if selected_name != user_names[active_index]:
            selected_user = next((u for u in users if u.get("name") == selected_name), None)
            if selected_user:
                set_active_user_id(selected_user.get("id"))
                st.rerun()

    with st.expander("Add user"):
        new_user_name = st.text_input("Name", key="new_user_name")
        if st.button("Create user", use_container_width=True):
            created = add_user(new_user_name)
            if created:
                st.success(f"User '{created.get('name')}' added.")
                st.rerun()
            else:
                st.warning("Enter a valid name.")

if run_now:
    report = run_daily()
    st.sidebar.success("Daily refresh completed.")
else:
    report = load_latest_report()

report = report or {}
generated_at = report.get("generated_at")

top_picks = report.get("top_picks_combined", report.get("top_picks", []))
top_picks_stocks = report.get("top_picks_stocks", [])
top_picks_bonds = report.get("top_picks_bonds", [])
outlook = report.get("outlook", {})
notes = report.get("notes", [])

holdings = load_portfolio(active_user_id)
portfolio_snapshot = portfolio_summary(holdings)

st.sidebar.divider()
st.sidebar.header("Status")
st.sidebar.caption(f"Last report: {format_timestamp(generated_at)}")
st.sidebar.caption(f"Top picks: {len(top_picks)}")
st.sidebar.caption(f"Holdings: {len(holdings)}")

if not report:
    st.info("No report found yet. Run Daily Refresh to generate the first report.")

tab_overview, tab_top, tab_outlook, tab_summary, tab_portfolio, tab_instrument = st.tabs(
    ["Overview", "Top Picks", "Market Outlook", "Summary", "Portfolio", "Instrument"]
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

    def render_picks(section_title: str, picks: list[dict]) -> None:
        st.markdown(f"**{section_title}**")
        df = pd.DataFrame(picks)
        if df.empty:
            st.info("No picks available yet.")
            return
        df = df.sort_values("score", ascending=False)

        min_conf = st.slider(
            "Minimum confidence",
            0.0,
            1.0,
            value=0.0,
            step=0.05,
            key=f"{section_title}-conf",
        )
        filtered = df.copy()
        if "confidence" in filtered.columns:
            filtered = filtered[filtered["confidence"] >= min_conf]

        st.caption(f"Showing {len(filtered)} of {len(df)} picks.")
        if filtered.empty:
            st.info("No picks match the current filters.")
            return

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

    sub_combined, sub_stocks, sub_bonds = st.tabs(["Combined", "Stocks", "Bonds"])

    with sub_combined:
        render_picks("Combined Picks (No ETFs)", top_picks)
    with sub_stocks:
        render_picks("Stock Picks", top_picks_stocks)
    with sub_bonds:
        render_picks("Bond Picks", top_picks_bonds)

with tab_outlook:
    st.subheader("Market Outlook")
    daily = outlook.get("daily", "No daily summary yet.")
    deep = outlook.get("deep", "No deep summary yet.")

    st.markdown("**Daily Pulse**")
    st.write(daily)
    st.divider()
    st.markdown("**Deeper View**")
    st.write(deep)

with tab_summary:
    st.subheader("Market Summary")
    universe_df = load_universe()
    imported_df = load_imported_universe()
    if universe_df.empty:
        st.info("Universe is empty; import instruments to see summary.")
    else:
        prices_df = load_prices(universe_df)
        st.markdown("**Universe Status**")

        total_universe = len(universe_df)
        imported_count = len(imported_df)
        summary_stats = report.get("summary", {}) if report else {}
        data_ready = summary_stats.get("data_ready")
        price_age = summary_stats.get("price_age_days")

        coverage_pct = 0.0
        median_history = None
        if not prices_df.empty and "instrument_id" in prices_df.columns:
            counts = prices_df.groupby("instrument_id").size()
            coverage_pct = float(counts.size / total_universe) if total_universe else 0.0
            median_history = float(counts.median()) if not counts.empty else None

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Universe Size", total_universe)
        col2.metric("Imported Size", imported_count)
        col3.metric("Price Coverage", format_metric(coverage_pct, "{:.0%}"))
        col4.metric("Price Age (days)", price_age if price_age is not None else "n/a")

        if data_ready is False:
            st.warning("Data readiness checks failed. Load the full universe and daily prices to enable summaries.")

        if not universe_df.empty and "asset_type" in universe_df.columns:
            type_counts = (
                universe_df["asset_type"].fillna("unknown").astype(str).str.lower().value_counts().reset_index()
            )
            type_counts.columns = ["asset_type", "count"]
            fig_types = px.bar(type_counts, x="asset_type", y="count", title="Asset type distribution")
            st.plotly_chart(fig_types, use_container_width=True)

        if median_history is not None:
            st.caption(f"Median history length: {int(median_history)} trading days.")
        if prices_df.empty:
            st.info("No price data available yet.")
        else:
            prices_df["date"] = pd.to_datetime(prices_df["date"])
            recent = prices_df.sort_values("date")
            latest_date = recent["date"].max()
            st.caption(f"Latest data: {latest_date.date()}")

            def compute_trend(window_days: int) -> float | None:
                cutoff = latest_date - pd.Timedelta(days=window_days)
                subset = recent[recent["date"] >= cutoff]
                if subset.empty:
                    return None
                pivot = subset.pivot_table(index="date", columns="instrument_id", values="close")
                returns = pivot.pct_change().dropna(how="all")
                if returns.empty:
                    return None
                avg_return = returns.mean(axis=1)
                cumulative = (1 + avg_return).prod() - 1
                return float(cumulative)

            short_1w = compute_trend(7)
            short_3w = compute_trend(21)
            long_1y = compute_trend(365)
            long_5y = compute_trend(365 * 5)

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("1W Trend", format_metric(short_1w, "{:.2%}") if short_1w is not None else "n/a")
            col2.metric("3W Trend", format_metric(short_3w, "{:.2%}") if short_3w is not None else "n/a")
            col3.metric("1Y Trend", format_metric(long_1y, "{:.2%}") if long_1y is not None else "n/a")
            col4.metric("5Y Trend", format_metric(long_5y, "{:.2%}") if long_5y is not None else "n/a")

    st.markdown("**Top Picks Context**")
    st.write(
        "Top picks are selected from the same universe with ETFs excluded. "
        "Short-term trends emphasize the last 1-3 weeks; long-term trends emphasize 1-5 years."
    )

    summary_stats = report.get("summary", {}) if report else {}
    imported_count = summary_stats.get("imported_universe_count")
    if imported_count is not None:
        st.caption(f"Imported universe size: {imported_count}")

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
    add_mode = st.radio("Add method", ["Search", "Manual"], horizontal=True)

    if add_mode == "Search":
        universe_df = load_universe()
        if not universe_df.empty:
            universe_df = universe_df[universe_df["asset_type"] != "etf"]

        search = st.text_input("Search by name, ticker, ISIN, or ID")
        candidates = universe_df.copy()
        if search and not candidates.empty:
            search_lower = search.strip().lower()
            mask = (
                candidates.get("name", "").astype(str).str.lower().str.contains(search_lower, na=False)
                | candidates.get("ticker", "").astype(str).str.lower().str.contains(search_lower, na=False)
                | candidates.get("isin", "").astype(str).str.lower().str.contains(search_lower, na=False)
                | candidates.get("instrument_id", "").astype(str).str.lower().str.contains(search_lower, na=False)
            )
            candidates = candidates[mask]

        candidates = candidates.head(50) if not candidates.empty else candidates

        if candidates.empty:
            st.info("No instruments matched your search.")
        else:
            options = [
                f"{row.get('name', 'Unknown')} ({row.get('instrument_id')})"
                for _, row in candidates.iterrows()
            ]
            selection = st.selectbox("Select instrument", options)
            weight = st.number_input(
                "Weight", min_value=0.0, max_value=1.0, value=0.1, step=0.01
            )
            if st.button("Add selected"):
                idx = options.index(selection)
                row = candidates.iloc[idx]
                holding = {
                    "instrument_id": str(row.get("instrument_id")).strip(),
                    "name": str(row.get("name")).strip(),
                    "weight": float(weight),
                }
                holdings.append(holding)
                save_portfolio(holdings, active_user_id)
                st.success("Holding added.")
                st.rerun()
    else:
        with st.form("add_holding_manual"):
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
                    save_portfolio(holdings, active_user_id)
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
            save_portfolio(holdings, active_user_id)
            st.success("Holding removed.")
            st.rerun()

with tab_instrument:
    st.subheader("Instrument View")
    universe_df = load_universe()
    if universe_df.empty:
        st.info("Universe is empty; import instruments to explore.")
    else:
        search_term = st.text_input("Search instruments")
        filtered = universe_df.copy()
        if search_term:
            needle = search_term.strip().lower()
            mask = (
                filtered.get("name", "").astype(str).str.lower().str.contains(needle, na=False)
                | filtered.get("ticker", "").astype(str).str.lower().str.contains(needle, na=False)
                | filtered.get("isin", "").astype(str).str.lower().str.contains(needle, na=False)
                | filtered.get("instrument_id", "").astype(str).str.lower().str.contains(needle, na=False)
            )
            filtered = filtered[mask]

        filtered = filtered.head(100) if not filtered.empty else filtered
        if filtered.empty:
            st.info("No instruments matched your search.")
        else:
            options = [
                f"{row.get('name', 'Unknown')} ({row.get('instrument_id')})"
                for _, row in filtered.iterrows()
            ]
            selected = st.selectbox("Select instrument", options)
            idx = options.index(selected)
            instrument = filtered.iloc[idx]
            instrument_id = str(instrument.get("instrument_id"))

            price_df = load_prices(universe_df, instrument_ids=[instrument_id])
            if price_df.empty:
                st.info("No price data available yet.")
            else:
                price_df["date"] = pd.to_datetime(price_df["date"])
                price_df = price_df.sort_values("date")
                fig = px.line(price_df, x="date", y="close", title="Price history")
                st.plotly_chart(fig, use_container_width=True)

                config = load_config()
                projection_days = int(config.get("projection_days", 90))
                recent = price_df.tail(90)
                if len(recent) >= 5:
                    returns = recent["close"].pct_change().dropna()
                    mu = returns.mean()
                    sigma = returns.std()
                    last_price = recent["close"].iloc[-1]
                    projected = [last_price]
                    projected_high = [last_price]
                    projected_low = [last_price]
                    for _ in range(projection_days):
                        projected.append(projected[-1] * (1 + mu))
                        projected_high.append(projected_high[-1] * (1 + mu + sigma))
                        projected_low.append(projected_low[-1] * (1 + mu - sigma))
                    proj_dates = pd.date_range(
                        start=price_df["date"].iloc[-1] + pd.Timedelta(days=1),
                        periods=projection_days + 1,
                        freq="B",
                    )
                    projection_df = pd.DataFrame(
                        {
                            "date": proj_dates,
                            "projection": projected,
                            "projection_high": projected_high,
                            "projection_low": projected_low,
                        }
                    )
                    fig_proj = go.Figure()
                    fig_proj.add_trace(
                        go.Scatter(
                            x=projection_df["date"],
                            y=projection_df["projection_low"],
                            line=dict(color="rgba(184, 205, 198, 0.0)"),
                            showlegend=False,
                            hoverinfo="skip",
                            name="Lower bound",
                        )
                    )
                    fig_proj.add_trace(
                        go.Scatter(
                            x=projection_df["date"],
                            y=projection_df["projection_high"],
                            fill="tonexty",
                            fillcolor="rgba(184, 205, 198, 0.35)",
                            line=dict(color="rgba(184, 205, 198, 0.0)"),
                            showlegend=False,
                            hoverinfo="skip",
                            name="Upper bound",
                        )
                    )
                    fig_proj.add_trace(
                        go.Scatter(
                            x=projection_df["date"],
                            y=projection_df["projection"],
                            line=dict(color="#4f6b5e", width=2),
                            name="Projection",
                        )
                    )
                    fig_proj.update_layout(title="Illustrative projection (range)")
                    st.plotly_chart(fig_proj, use_container_width=True)
                    st.caption(
                        "Projection is illustrative and based on recent average returns; not investment advice."
                    )
                else:
                    st.info("Not enough recent data to build a projection.")
