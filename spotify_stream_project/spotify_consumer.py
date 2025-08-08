from kafka import KafkaConsumer
import json

# Kafka consumer
consumer = KafkaConsumer(
    'spotify-raw',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# PostgreSQL connection

import psycopg2
import os

DATABASE_URL = "postgresql://postgres:Sri%2F123%40@localhost:5432/spotify_stream"

conn = psycopg2.connect(DATABASE_URL)

# conn = psycopg2.connect(
#     dbname="project",
#     user="postgres",
#     password="pass",
#     host="localhost",
#     port="5432"
# )
cursor = conn.cursor()

print("Consuming messages and inserting into DB...")

for message in consumer:
    try:
        data = message.value
        cursor.execute("""
            INSERT INTO streamed_tracks (timestamp, track, artist, album, duration_ms, progress_ms, is_playing)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            data.get('timestamp'),
            data.get('track'),
            data.get('artist'),
            data.get('album'),
            data.get('duration_ms'),
            data.get('progress_ms'),
            data.get('is_playing')
        ))
        conn.commit()
        print(f"Inserted into DB: {data['track']} by {data['artist']}")
    except Exception as e:
        print("Error:", e)
        conn.rollback()
