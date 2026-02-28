import duckdb
import streamlit as st

@st.cache_resource
def get_connection():
    return duckdb.connect("analytics/analytics.duckdb", read_only=True)
