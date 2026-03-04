## Music Analytics Platform 
A real-time data engineering and analytics pipeline that ingests streaming music events, processes them with Apache Spark, stores curated data in an Iceberg warehouse, and exposes behavioral and content intelligence dashboards through Streamlit.
This project simulates a music streaming platform's event flow (plays, skips, likes, sessions) and transforms raw streaming data into analytics-ready datasets for product and content insights.

## Architecture

The system follows a modern streaming analytics architecture:
Client Events → Flask Ingestion API → Kafka → Spark Structured Streaming → Iceberg Data Warehouse → DuckDB Analytics Layer → Streamlit Dashboard

Pipeline components:
  1. Event ingestion through an HTTP API
  2. Kafka streaming event pipeline
  3.Spark Structured Streaming processing
  4. Iceberg data warehouse storage
  5. DuckDB analytical querying
  6. Streamlit visualization layer
