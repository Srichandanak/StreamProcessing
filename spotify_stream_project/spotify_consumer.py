# spotify_consumer.py
from kafka import KafkaConsumer
import json
from collections import Counter
import threading

# In-memory stats
song_counter = Counter()
artist_counter = Counter()
total_tracks = 0
total_duration = 0  # in ms

def consume():
    global total_tracks, total_duration
    consumer = KafkaConsumer(
        'spotify-raw',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        group_id='spotify-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    for message in consumer:
        data = message.value
        if data['is_playing']:
            song_counter[data['track']] += 1
            artist_counter[data['artist']] += 1
            total_tracks += 1
            total_duration += data['duration_ms']

# Start consuming in background when imported
threading.Thread(target=consume, daemon=True).start()
