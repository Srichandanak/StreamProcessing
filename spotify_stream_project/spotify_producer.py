import json
import time
from kafka import KafkaProducer
import spotipy
from spotipy.oauth2 import SpotifyOAuth

# Set up Spotify authentication
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id="c6441919302447a087efb7d309dbc324",
    client_secret="a4bed5de29bd40adac0ca42cdbcbd067",
    redirect_uri="http://127.0.0.1:8888/callback",
    scope="user-read-playback-state user-read-currently-playing"
))

# Set up Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Infinite loop to send currently playing track to Kafka
while True:
    try:
        playback = sp.current_playback()

        if playback and playback['is_playing']:
            song_data = {
                'timestamp': playback['timestamp'],
                'track': playback['item']['name'],
                'artist': playback['item']['artists'][0]['name'],
                'album': playback['item']['album']['name'],
                'duration_ms': playback['item']['duration_ms'],
                'progress_ms': playback['progress_ms'],
                'is_playing': playback['is_playing']
            }

            print("Sending to Kafka:", song_data)
            producer.send('spotify-raw', value=song_data)
        else:
            print("No song is currently playing.")

    except Exception as e:
        print("Error:", e)

    time.sleep(5)  # Wait 5 seconds before checking again
