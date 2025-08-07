# spotify_kafka_producer.py

import time
import json
from kafka import KafkaProducer
import spotipy
from spotipy.oauth2 import SpotifyOAuth

# Spotify Auth (use your actual credentials here)
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id='c6441919302447a087efb7d309dbc324',
    client_secret='a4bed5de29bd40adac0ca42cdbcbd067',
    redirect_uri='http://127.0.0.1:8888/callback',
    scope='user-read-recently-played'
))

# Kafka Producer Setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# To track already seen songs
seen_ids = set()

while True:
    print("⏳ Fetching recently played tracks...")
    recent = sp.current_user_recently_played(limit=10)

    for item in recent['items']:
        track = item['track']
        track_id = track['id']

        if track_id not in seen_ids:
            seen_ids.add(track_id)

            payload = {
                'track_name': track['name'],
                'artist': track['artists'][0]['name'],
                'played_at': item['played_at'],
                'album': track['album']['name']
            }

            print(f"🎶 Sending track: {payload['track_name']} by {payload['artist']}")
            producer.send('spotify-raw', value=payload)

    time.sleep(30)  # wait before next fetch
