import streamlit as st
import sys
import numpy as np
import altair as alt
import pandas as pd

sys.path.insert(0, ".")
from analytics.queries import (
    get_user_behavior_kpis,
    get_user_play_distribution,
    get_unique_track_distribution,
    get_user_session_distribution
)

def show_user_behaviour(selected_date):
    st.title("User Behaviour")
    st.markdown("""
        ### User KPIs
        Snapshot of daily user engagement behavior based on activity patterns and listening depth.
        """)
    kpi_df = get_user_behavior_kpis(selected_date)

    if not kpi_df.empty:
        row = kpi_df.iloc[0]
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Explorer Users %", f"{row['explorer_pct']:.1f}%")
        col2.metric("Multi-session %", f"{row['multi_session_pct']:.1f}%")
        col3.metric("Heavy Engagement %", f"{row['heavy_engagement_pct']:.1f}%")
        col4.metric("Avg Unique Tracks", f"{row['avg_unique_tracks']:.1f}")

    st.markdown("""
## Plays Per User
Shows the distribution of total plays generated per user on the selected date, illustrating how listening activity is spread across light, moderate, and heavy users.""")
    play_df = get_user_play_distribution(selected_date)

    chart = (
        alt.Chart(play_df)
        .mark_bar()
        .encode(
            x=alt.X("plays:Q", bin=True, title="Plays per User"),
            y=alt.Y("count()", title="User Count")
        )
    )

    st.altair_chart(chart, use_container_width=True)

    st.markdown("""
                ## Sessions Distribution
                   Displays the distribution of total plays per user, highlighting engagement intensity and identifying heavy listeners.
 """)
    session_df = get_user_session_distribution(selected_date)

    if not session_df.empty:
        bins = [0, 1, 2, 3, 5, 10]
        labels = ["1", "2", "3", "4-5", "6+"]

        session_df["bucket"] = pd.cut(
            session_df["sessions"],
            bins=bins,
            labels=labels,
            right=True
        )

        bucket_counts = (
            session_df["bucket"]
            .value_counts()
            .sort_index()
            .reset_index()
        )

        bucket_counts.columns = ["Session Bucket", "User Count"]

        chart = (
            alt.Chart(bucket_counts)
            .mark_bar(size=40)
            .encode(
                x=alt.X("Session Bucket:N", title="Sessions per User"),
                y=alt.Y("User Count:Q", title="Number of Users"),
                tooltip=["Session Bucket", "User Count"]
            )
            .properties(height=400)
        )

        st.altair_chart(chart, use_container_width=True)

    else:
        st.info("No session data available.")
    # ------------------------------------------------------
    st.markdown("""
                ## Unique Tracks Per User
                Shows the distribution of distinct tracks listened to per user, highlighting content exploration patterns and listening breadth.""")

    uniq_df = get_unique_track_distribution(selected_date)

    chart = (
        alt.Chart(uniq_df)
        .mark_bar()
        .encode(
            x=alt.X("unique_tracks:Q", bin=True, title="Unique Tracks per User"),
            y=alt.Y("count()", title="User Count")
        )
    )

    st.altair_chart(chart, use_container_width=True)