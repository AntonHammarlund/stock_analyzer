import streamlit as st
import pandas as pd
import plotly.express as px

from stock_analyzer.pipeline import run_daily
from stock_analyzer.reports import load_latest_report
from stock_analyzer.portfolio import load_portfolio, save_portfolio

st.set_page_config(page_title="Stock Analyzer MVP", layout="wide")

st.title("Stock Analyzer MVP")

if st.button("Run Daily Refresh"):
    report = run_daily()
    st.success("Daily refresh completed.")
else:
    report = load_latest_report()

if not report:
    st.info("No report found yet. Click 'Run Daily Refresh' to generate.")


tab_top, tab_outlook, tab_portfolio = st.tabs(["Top Picks", "Market Outlook", "Portfolio"])

with tab_top:
    st.subheader("Top Picks")
    top_picks = report.get("top_picks", []) if report else []
    if not top_picks:
        st.warning("No top picks available yet.")
    else:
        df = pd.DataFrame(top_picks)
        st.dataframe(df, use_container_width=True)
        fig = px.bar(df, x="name", y="score", title="Top Picks Scores")
        st.plotly_chart(fig, use_container_width=True)

with tab_outlook:
    st.subheader("Market Outlook")
    outlook = report.get("outlook", {}) if report else {}
    st.write(outlook.get("daily", "No daily summary yet."))
    st.divider()
    st.write(outlook.get("deep", "No deep summary yet."))

with tab_portfolio:
    st.subheader("Portfolio")
    portfolio = load_portfolio()

    if portfolio:
        df = pd.DataFrame(portfolio)
        st.dataframe(df, use_container_width=True)
    else:
        st.info("No holdings added yet.")

    st.markdown("### Add Holding")
    with st.form("add_holding"):
        instrument_id = st.text_input("Instrument ID")
        name = st.text_input("Name")
        weight = st.number_input("Weight", min_value=0.0, max_value=1.0, value=0.1, step=0.01)
        submitted = st.form_submit_button("Add")
        if submitted:
            holding = {"instrument_id": instrument_id, "name": name, "weight": weight}
            portfolio.append(holding)
            save_portfolio(portfolio)
            st.success("Holding added.")

    if portfolio:
        st.markdown("### Remove Holding")
        options = [f"{h.get('instrument_id')} - {h.get('name')}" for h in portfolio]
        to_remove = st.selectbox("Select holding", options)
        if st.button("Remove"):
            index = options.index(to_remove)
            portfolio.pop(index)
            save_portfolio(portfolio)
            st.success("Holding removed.")
