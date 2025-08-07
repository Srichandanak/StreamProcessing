# spotify_stats_backend.py
from fastapi import FastAPI
from spotify_consumer import song_counter, artist_counter, total_tracks, total_duration

app = FastAPI()

@app.get("/")
def root():
    return {"message": "Spotify Stats API is running!"}

@app.get("/stats")
def get_stats():
    return {
        "total_tracks": total_tracks,
        "total_duration_minutes": round(total_duration / 60000, 2),
        "most_played_songs": song_counter.most_common(5),
        "most_played_artists": artist_counter.most_common(5),
    }
