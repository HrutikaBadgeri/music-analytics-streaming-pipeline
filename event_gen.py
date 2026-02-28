import os 
import time
from flask import Flask, app, request
from datetime import datetime, timezone
from flask import jsonify
from kafka import KafkaProducer
import json 
import uuid
import random
import string


app = Flask(__name__)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

KAFKA_TOPIC = "events"

REQUIRED_FIELDS = [
    "event_type"
]

@app.route("/event", methods=["POST"])
def ingest_event():
    payload = request.get_json()

    if not payload:
        return jsonify({"error": "Invalid JSON"}), 400

    for field in REQUIRED_FIELDS:
        if field not in payload:
            return jsonify({"error": f"Missing field: {field}"}), 400
    
    # if payload.event_type == "play" or "PLAY":
    #     payload.session_id  = str(uuid.uuid4())
    
    event = {
        "event_id": str(uuid.uuid4()),
        "ingestion_time": datetime.now(timezone.utc).isoformat(),
        **payload
    }

    # Push to Kafka
    producer.send(KAFKA_TOPIC, value=event)
    producer.flush()

    return jsonify({
        "status": "success",
        "event_id": event["event_id"]
    }), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)