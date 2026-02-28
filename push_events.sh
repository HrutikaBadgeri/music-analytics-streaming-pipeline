#!/bin/bash

URL="http://localhost:5000/event"

SESSION_ID=$(uuidgen)

echo "Using session: $SESSION_ID"

for i in {1..20}
do

EVENT_TYPE=$((RANDOM % 3))

if [ $EVENT_TYPE -eq 0 ]; then

# PLAY EVENT
curl -s -X POST $URL \
-H "Content-Type: application/json" \
-d "{
  \"event_id\": \"$(uuidgen)\",
  \"event_type\": \"play\",
  \"client_event_time\": \"$(date -u +"%Y-%m-%dT%H:%M:%SZ")\",
  \"session_id\": \"$SESSION_ID\",
  \"user_data\": {
    \"user_id\": \"user_$((RANDOM % 5))\",
    \"country\": \"US\",
    \"subscription_plan\": \"premium\",
    \"device_type\": \"smartphone\",
    \"platform\": \"ios\"
  },
  \"content_data\": {
    \"track_id\": \"track_$((RANDOM % 100))\",
    \"artist_id\": $((RANDOM % 50)),
    \"album_id\": $((RANDOM % 20)),
    \"duration_ms\": 210000
  }
}"

echo "Sent PLAY event"

elif [ $EVENT_TYPE -eq 1 ]; then

# SKIP EVENT
curl -s -X POST $URL \
-H "Content-Type: application/json" \
-d "{
  \"event_id\": \"$(uuidgen)\",
  \"event_type\": \"skip\",
  \"client_event_time\": \"$(date -u +"%Y-%m-%dT%H:%M:%SZ")\",
  \"session_id\": \"$SESSION_ID\",
  \"user_data\": {
    \"user_id\": \"user_$((RANDOM % 5))\"
  },
  \"content_data\": {
    \"track_id\": \"track_$((RANDOM % 100))\",
    \"artist_id\": $((RANDOM % 50)),
    \"album_id\": $((RANDOM % 20)),
    \"duration_ms\": 210000
  },
  \"playback\": {
    \"played_ms\": $((RANDOM % 200000)),
    \"reason_skip\": \"user\"
  }
}"

echo "Sent SKIP event"

else

# APP_CLOSE EVENT
curl -s -X POST $URL \
-H "Content-Type: application/json" \
-d "{
  \"event_id\": \"$(uuidgen)\",
  \"event_type\": \"app_close\",
  \"client_event_time\": \"$(date -u +"%Y-%m-%dT%H:%M:%SZ")\",
  \"session_id\": \"$SESSION_ID\",
  \"user_data\": {
    \"user_id\": \"user_$((RANDOM % 5))\"
  },
  \"context\": {
    \"close_reason\": \"user_exit\"
  }
}"

echo "Sent APP_CLOSE event"

fi

sleep 0.5

done

echo "✅ Finished sending events!"
