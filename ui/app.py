# from pages import platform_overview
import streamlit as st
from _pages.platform_overview import show_platform_overview
from _pages.tracks_performance import show_tracks_performance
from _pages.user_behaviour import show_user_behaviour
import duckdb
from datetime import date, timedelta

st.set_page_config(
    page_title="Music Analytics Dashboard",
    page_icon="🎧",
    layout="wide"
)

st.sidebar.title("Music Analytics")

st.sidebar.markdown("---")

# Page Navigation
page = st.sidebar.radio(
    "Navigate",
    (
        "Platform Overview",
        "Track Intelligence",
        "User Behavior"
    )
)

st.sidebar.markdown("---")
selected_date = date.today() - timedelta(days=1)
# Date Selector
selected_date = st.sidebar.date_input(
    "Select Date",
    value=date.today()
)

st.sidebar.markdown("---")

st.sidebar.info(
    """
    Data Source:
    Iceberg + DuckDB  
    Streaming via Kafka  
    """
)
if page == "Platform Overview":
    show_platform_overview(selected_date)

elif page == "Track Intelligence":
    show_tracks_performance(selected_date)

elif page == "User Behavior":
    show_user_behaviour(selected_date)
    