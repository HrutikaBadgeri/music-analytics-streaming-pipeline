import streamlit as st
import sys
import numpy as np
import pandas as pd
import altair as alt


sys.path.insert(0, ".")
from analytics.queries import (
    get_dau,
    get_total_plays,
    get_heavy_user_pct,
    get_engagement_trend,
    get_user_play_distribution,
    get_user_session_distribution,
    get_plan_distribution
)

def show_platform_overview(selected_date):

    st.title("Platform Overview")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            label="Daily Active Users",
            value=get_dau(selected_date)
        )

    with col2:
        st.metric(
            label="Total Plays",
            value=get_total_plays(selected_date)
        )

    with col3:
        st.metric(
            label="Heavy Users %",
            value=f"{get_heavy_user_pct(selected_date)}%"
        )

    ## line chart for engagement trend
    col1, col2 = st.columns([4, 1])
    with col1:
        st.markdown("## Engagement Trend")
        st.markdown("#### Total plays per day (platform-level)")

    with col2:
        date_range = st.selectbox(
            "Range",
            ("Last 7 Days", "Last 30 Days", "All Time"),
            label_visibility="collapsed"
        )
    
    if date_range == "Last 7 Days":
        trend_df = get_engagement_trend(7)
    elif date_range == "Last 30 Days":
        trend_df = get_engagement_trend(30)
    else:
        trend_df = get_engagement_trend()
    
    if not trend_df.empty:
        trend_df["rolling_avg"] = (
            trend_df["total_plays"]
            .rolling(window=7, min_periods=1)
            .mean()
        )
        st.line_chart(
            trend_df,
            x="date",
            y=["total_plays", "rolling_avg"],
            use_container_width=True
        )
    else:
        st.info("No engagement data available.")

    # user activity
    st.markdown("## User Activity Distribution")
    st.markdown("#### Plays per user (engagement-tier)")
    dist_df = get_user_play_distribution(selected_date)

    if not dist_df.empty:
        bins = [0, 5, 20, 50, 100, np.inf]
        labels = ["1-5", "6-20", "21-50", "51-100", "100+"]
        dist_df["play_bucket"] = pd.cut(
            dist_df["plays"],
            bins=bins,
            labels=labels,
            right=True
        )
        bucket_counts = (
            dist_df["play_bucket"]
            .value_counts()
            .sort_index()
            .reset_index()
        )
        bucket_counts.columns = ["Engagement Tier", "User Count"]
        chart = (
            alt.Chart(bucket_counts)
            .mark_bar(size=40)
            .encode(
                x=alt.X("Engagement Tier:N", title="Plays per User"),
                y=alt.Y("User Count:Q", title="Number of Users"),
                tooltip=["Engagement Tier", "User Count"]
            )
            .properties(height=350)
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No user activity data available.")
        

    # session activity
    st.markdown("## Sessions Distribution")
    st.markdown('#### How often active users returned to the app today/daily session frequency of active users')
    session_df = get_user_session_distribution(selected_date)
    if not session_df.empty:
        bins = [0, 1, 2, 3, 5, 10, np.inf]
        labels = ["1", "2", "3", "4-5", "6-10", "10+"]
        session_df["session_bucket"] = pd.cut(
            session_df["sessions"],
            bins=bins,
            labels=labels,
            right=True
        )
        bucket_counts = (
            session_df["session_bucket"]
            .value_counts()
            .sort_index()
            .reset_index()
        )
        bucket_counts.columns = ["Session Tier", "User Count"]
        # Convert to percentage
        bucket_counts["User %"] = (
            bucket_counts["User Count"] /
            bucket_counts["User Count"].sum()
        ) * 100

        chart = (
            alt.Chart(bucket_counts)
            .mark_bar(size=40)
            .encode(
                x=alt.X("Session Tier:N", title="Sessions per User"),
                y=alt.Y("User %:Q", title="Percentage of Active Users"),
                tooltip=[
                    "Session Tier",
                    alt.Tooltip("User %:Q", format=".1f"),
                    "User Count"
                ]
            )
            .properties(height=350)
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No session data available.")
    
    st.markdown("## Plan Distribution")
    st.markdown("#### plan distribution as per different types")
    plan_df = get_plan_distribution()
    chart = (
        alt.Chart(plan_df)
        .mark_arc(innerRadius=60)
        .encode(
            theta=alt.Theta("user_count:Q", title="Users"),
            color=alt.Color("plan_type:N", title="Plan Type"),
            tooltip=["plan_type", "user_count"]
        )
    )
    st.altair_chart(chart, use_container_width=True)
