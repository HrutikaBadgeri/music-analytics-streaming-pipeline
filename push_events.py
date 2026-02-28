import requests
import random
import uuid
from datetime import datetime, timedelta

API_URL = "http://localhost:5000/event"

NUM_SESSIONS = 150
NUM_USERS = 20

# -----------------------
# FIXED USERS & TRACKS
# -----------------------
users = [f"user_{i}" for i in range(1, NUM_USERS + 1)]

popular_tracks = ["track_101", "track_102", "track_103"]
normal_tracks = ["track_104", "track_105", "track_106", "track_107", "track_108"]
hidden_tracks = ["track_109", "track_110"]

countries = ["US", "India", "UK", "Germany", "Canada"]
plans = ["free", "premium", "family", "student"]
devices = ["smartphone", "desktop"]
platforms = ["ios", "android", "web"]

# -----------------------
# HELPERS
# -----------------------
def weighted_track():
    r = random.random()
    if r < 0.5:
        return random.choice(popular_tracks)
    elif r < 0.9:
        return random.choice(normal_tracks)
    else:
        return random.choice(hidden_tracks)

def random_user_data():
    return {
        "user_id": random.choice(users),
        "country": random.choice(countries),
        "subscription_plan": random.choice(plans),
        "device_type": random.choice(devices),
        "platform": random.choice(platforms),
    }

def now_iso(offset=0):
    return (datetime.utcnow() - timedelta(minutes=offset)).isoformat() + "Z"

def session_plays():
    r = random.random()
    if r < 0.5:
        return random.randint(1, 2)   # casual
    elif r < 0.85:
        return random.randint(2, 4)   # regular
    else:
        return random.randint(5, 7)   # heavy

def send_event(payload):
    requests.post(API_URL, json=payload)

# -----------------------
# SESSION SIMULATION
# -----------------------
for _ in range(NUM_SESSIONS):
    session_id = str(uuid.uuid4())
    user_data = random_user_data()

    # app_open
    send_event({
        "event_type": "app_open",
        "client_event_time": now_iso(),
        "session_id": session_id,
        "user_data": user_data
    })

    plays = session_plays()

    for _ in range(plays):
        track_id = weighted_track()

        # play
        send_event({
            "event_type": "play",
            "client_event_time": now_iso(),
            "session_id": session_id,
            "user_data": user_data,
            "content_data": {
                "track_id": track_id,
                "artist_id": random.randint(1, 20),
                "album_id": random.randint(1, 10),
                "duration_ms": random.randint(180000, 240000)
            }
        })

        # skip
        if random.random() < 0.25:
            send_event({
                "event_type": "skip",
                "client_event_time": now_iso(),
                "session_id": session_id,
                "user_data": user_data,
                "content_data": {"track_id": track_id},
                "playback": {"played_ms": random.randint(10000, 60000), "reason_skip": "user"}
            })

        # like
        if random.random() < 0.2:
            send_event({
                "event_type": "like",
                "client_event_time": now_iso(),
                "session_id": session_id,
                "user_data": user_data,
                "content_data": {"track_id": track_id}
            })

        # playlist add
        if random.random() < 0.15:
            send_event({
                "event_type": "add_to_playlist",
                "client_event_time": now_iso(),
                "session_id": session_id,
                "user_data": user_data,
                "content_data": {"track_id": track_id},
                "context": {"playlist_id": f"playlist_{random.randint(1,5)}"}
            })

    # app_close
    send_event({
        "event_type": "app_close",
        "client_event_time": now_iso(),
        "session_id": session_id,
        "user_data": {"user_id": user_data["user_id"]},
        "context": {"close_reason": "user_exit"}
    })

print("Simulation complete!")