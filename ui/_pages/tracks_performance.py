import streamlit as st
import sys
import numpy as np
import altair as alt
# import altair as alt
import pandas as pd

sys.path.insert(0, ".")
from analytics.queries import (
    get_top_tracks,
    get_most_loved_tracks,
    get_track_engagement_reach
)

def show_tracks_performance(selected_date):
    st.title("Music Tracks Performance")

    st.markdown("### Top Tracks Today")
    st.markdown("#### Most played tracks on the platform for the selected date, ranked by total plays.")
    top_tracks_df = get_top_tracks(selected_date)
    print(top_tracks_df)
    if not top_tracks_df.empty:
        st.dataframe(
            top_tracks_df,
            use_container_width=True
        )
    else:
        st.info("No track data available.")

    st.markdown("Most Loved Tracks")
    loved_df = get_most_loved_tracks(selected_date)

    if not loved_df.empty:
        st.dataframe(loved_df, use_container_width=True)
    else:
        st.info("No love data available.")

    chart = (
    alt.Chart(loved_df)
    .mark_bar()
    .encode(
        x=alt.X("love_score:Q", title="Love Score (Likes + Playlist Adds)"),
        y=alt.Y("track_name:N", sort="-x", title="Track"),
        tooltip=["track_name", "likes", "playlist_adds", "love_score"]
    )
    .properties(height=400)
)

    st.altair_chart(chart, use_container_width=True)
    st.markdown("""
        ## Engagement vs Reach
        - Visualizes track performance by comparing listener reach and engagement quality.  
        - Each point represents a track, bubble size indicates total plays, and color reflects skip rate.  
        - This helps identify hits and underperforming tracks.
        """)
    scatter_df = get_track_engagement_reach(selected_date)
    chart = (
    alt.Chart(scatter_df)
    .mark_circle(size=80)
    .encode(
        x=alt.X("unique_listeners:Q", title="Reach (Unique Listeners)"),
        y=alt.Y("engagement_score:Q", title="Engagement Score"),
        size=alt.Size("plays:Q", title="Plays"),
        color=alt.Color("skip_rate:Q", title="Skip Rate"),
        tooltip=[
            "track_name",
            "unique_listeners",
            "engagement_score",
            "plays",
            "skip_rate"
        ]
    )
    .properties(height=450)
)

    st.altair_chart(chart, use_container_width=True)

