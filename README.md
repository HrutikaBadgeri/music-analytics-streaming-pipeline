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

## Tech Stack

| Layer | Technology |
|------|------------|
| Event Ingestion | Flask |
| Streaming | Apache Kafka |
| Stream Processing | Apache Spark Structured Streaming |
| Data Lakehouse | Apache Iceberg |
| Analytics Engine | DuckDB |
| Visualization | Streamlit |
| Language | Python |

## Event Data Model

The system simulates typical music platform user events.

### Play Event

``` json
{
  "event_type": "play",
  "client_event_time": "2024-05-20T14:30:05Z",
  "session_id": "sess-987654",
  "user_data": {
    "user_id": "user_101",
    "country": "US",
    "subscription_plan": "premium"
  },
  "content_data": {
    "track_id": "track_42",
    "artist_id": 12,
    "album_id": 5
  }
}
```
Other supported events:

-   app_open\
-   app_close\
-   play\
-   skip\
-   like\
-   add_to_playlist

# How to Run the Project

## 1. Start Kafka

Ensure Kafka is running locally.

## 2. Start Event Ingestion API

    python event_generation.py

## 3. Run Spark Streaming Jobs

Example:

    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2 raw_events.py

Run additional jobs for:

-   user_activity.py\
-   track_activity.py

## 4. Generate Synthetic Events

    python simulate_events.py

## 5. Launch Dashboard

    streamlit run ui/app.py

------------------------------------------------------------------------

# Key Analytics Concepts Implemented

This project demonstrates several real-world analytics patterns:

-   Behavioral segmentation
-   Content engagement scoring
-   Exploration vs repeat listening analysis
-   Reach vs engagement intelligence
-   Streaming event processing
-   Lakehouse-based analytics architecture

------------------------------------------------------------------------

### Dashboard Preview

### Platform Activity Overview
<img width="1728" height="1087" alt="image" src="https://github.com/user-attachments/assets/af30bebb-021c-42b6-a36a-16c8f2abb318" />
<img width="1728" height="1083" alt="image" src="https://github.com/user-attachments/assets/b69cb8c2-fecd-40b3-a0e0-23848cbf85b1" />
<img width="1728" height="1086" alt="image" src="https://github.com/user-attachments/assets/193d1756-dc7c-43ae-8f3b-67eed213a548" />

### Music Tracks Overview 
<img width="1728" height="1081" alt="image" src="https://github.com/user-attachments/assets/85a42d14-bcdd-4b4d-86c5-755ba0593eaa" />
<img width="1728" height="1076" alt="image" src="https://github.com/user-attachments/assets/d65d7f49-4ffa-4cb6-b196-6697e6abe279" />

### User Behaviour
<img width="1728" height="1082" alt="image" src="https://github.com/user-attachments/assets/554e18cd-edde-4b12-84c4-f75057025825" />
<img width="1719" height="1077" alt="image" src="https://github.com/user-attachments/assets/8f556773-dc92-4dc1-81e7-472fece69792" />








## License

MIT License


