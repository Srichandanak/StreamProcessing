# spotify_stats_backend.py

from fastapi import FastAPI
from dotenv import load_dotenv
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
import calendar

# Load environment variables
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

app = FastAPI()

NORMALIZATION_FACTOR = 18.0  # Estimated overcount factor (3 min song / 10s polling)

def get_db_connection():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

@app.get("/")
def root():
    return {"message": "Spotify Stats API is running!"}

@app.get("/stats")
def get_stats():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(DISTINCT dedupe_hash) AS total FROM streamed_tracks;")
    total_tracks = cur.fetchone()["total"]

    cur.execute("SELECT SUM(progress_ms) AS total_ms FROM streamed_tracks;")
    total_duration = cur.fetchone()["total_ms"] or 0

    cur.execute("""
        SELECT track, artist, ROUND(COUNT(DISTINCT dedupe_hash) / %s) AS count 
        FROM streamed_tracks 
        GROUP BY track, artist 
        ORDER BY count DESC 
        LIMIT 5;
    """, (NORMALIZATION_FACTOR,))
    top_songs = cur.fetchall()

    cur.execute("""
        SELECT artist, ROUND(COUNT(DISTINCT dedupe_hash) / %s) AS count 
        FROM streamed_tracks 
        GROUP BY artist 
        ORDER BY count DESC 
        LIMIT 5;
    """, (NORMALIZATION_FACTOR,))
    top_artists = cur.fetchall()

    cur.execute("""
        SELECT album, ROUND(COUNT(DISTINCT dedupe_hash) / %s) AS count 
        FROM streamed_tracks 
        GROUP BY album 
        ORDER BY count DESC 
        LIMIT 5;
    """, (NORMALIZATION_FACTOR,))
    top_albums = cur.fetchall()

    cur.close()
    conn.close()

    return {
        "total_tracks": total_tracks,
        "total_duration_minutes": round(total_duration / 60000, 2),
        "most_played_songs": top_songs,
        "most_played_artists": top_artists,
        "most_played_albums": top_albums
    }

@app.get("/stats/frequency-heatmap")
def get_frequency_heatmap():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT 
            EXTRACT(DOW FROM TO_TIMESTAMP(timestamp / 1000)) AS day, 
            EXTRACT(HOUR FROM TO_TIMESTAMP(timestamp / 1000)) AS hour, 
            ROUND(COUNT(*) / %s) AS count
        FROM streamed_tracks 
        GROUP BY day, hour;
    """, (NORMALIZATION_FACTOR,))

    heatmap = cur.fetchall()
    cur.close()
    conn.close()
    return {"heatmap": heatmap}

@app.get("/stats/streaks")
def get_listening_streak():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
    SELECT DISTINCT DATE(TO_TIMESTAMP(timestamp / 1000.0)) AS day 
    FROM streamed_tracks 
    ORDER BY day;
    """)
    days = [row["day"] for row in cur.fetchall()]

    longest = current = 1
    for i in range(1, len(days)):
        if (days[i] - days[i-1]).days == 1:
            current += 1
            longest = max(longest, current)
        else:
            current = 1

    cur.close()
    conn.close()
    return {"longest_streak_days": longest}

@app.get("/stats/all-songs")
def get_all_songs():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            track,
            artist,
            album,
            ROUND(COUNT(DISTINCT dedupe_hash) / %s) AS play_count,
            ROUND(SUM(progress_ms) / %s) AS total_ms
        FROM streamed_tracks
        GROUP BY track, artist, album
        ORDER BY play_count DESC;
    """, (NORMALIZATION_FACTOR, NORMALIZATION_FACTOR))
    all_songs = cur.fetchall()
    cur.close()
    conn.close()
    return {"songs": all_songs}

@app.get("/stats/summary")
def get_summary(period: str = "week"):
    conn = get_db_connection()
    cur = conn.cursor()

    today = datetime.utcnow().date()
    if period == "day":
        start = today
    elif period == "week":
        start = today - timedelta(days=today.weekday())
    elif period == "month":
        start = today.replace(day=1)
    elif period == "year":
        start = today.replace(month=1, day=1)
    else:
        return {"error": "Invalid period. Choose from day/week/month/year."}

    cur.execute("""
        SELECT ROUND(COUNT(DISTINCT dedupe_hash) / %s) AS play_count, 
               ROUND(SUM(progress_ms) / %s) AS total_ms
        FROM streamed_tracks 
        WHERE played_at >= %s;
    """, (NORMALIZATION_FACTOR, NORMALIZATION_FACTOR, start))
    data = cur.fetchone()

    cur.close()
    conn.close()

    return {
        "period": period,
        "play_count": data["play_count"],
        "total_duration_minutes": round((data["total_ms"] or 0) / 60000, 2)
    }
