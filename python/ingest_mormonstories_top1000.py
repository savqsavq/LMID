import os
import sqlite3
import time
import random

from googleapiclient.discovery import build
import whisper
import torch

from audio_path import download_audio

torch.set_num_threads(8)
torch.set_num_interop_threads(8)

API_KEY = "REDACTED"
DB_PATH = os.path.abspath("/Volumes/SS 1TB/LMID_ex/Analysis/Everything SQL db/lmid.db")

MAX_VIDEOS = 1000
WHISPER_MODEL = "tiny"


def ensure_longform_tables():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS longform_videos (
            video_id TEXT PRIMARY KEY,
            title TEXT,
            published_at TEXT,
            duration INTEGER
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS longform_transcripts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT,
            start REAL,
            end REAL,
            text TEXT,
            FOREIGN KEY(video_id) REFERENCES longform_videos(video_id)
        );
    """)

    conn.commit()
    conn.close()


def get_mormonstories_channel_id(youtube):
    resp = youtube.search().list(
        part="id,snippet",
        q="Mormon Stories Podcast",
        type="channel",
        maxResults=1
    ).execute()

    if not resp["items"]:
        raise RuntimeError("Channel not found.")

    return resp["items"][0]["id"]["channelId"]


def get_uploads_playlist_id(youtube, channel_id):
    resp = youtube.channels().list(
        part="contentDetails",
        id=channel_id
    ).execute()

    if not resp["items"]:
        raise RuntimeError("Missing channel details.")

    return resp["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]


def get_playlist_video_ids(youtube, playlist_id):
    ids = []
    next_page = None

    while True:
        resp = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page
        ).execute()

        for item in resp.get("items", []):
            ids.append(item["contentDetails"]["videoId"])

        next_page = resp.get("nextPageToken")
        if not next_page:
            break

    return ids


def fetch_video_metadata_batch(youtube, video_ids):
    meta = []

    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i+50]

        resp = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=",".join(chunk),
            maxResults=50
        ).execute()

        for item in resp.get("items", []):
            snippet = item.get("snippet", {})
            content = item.get("contentDetails", {})
            stats = item.get("statistics", {})

            duration_iso = content.get("duration")
            if not duration_iso:
                continue

            hours = minutes = seconds = 0
            digits = ""

            for char in duration_iso.replace("PT", ""):
                if char.isdigit():
                    digits += char
                else:
                    if char == "H":
                        hours = int(digits)
                    elif char == "M":
                        minutes = int(digits)
                    elif char == "S":
                        seconds = int(digits)
                    digits = ""

            duration_seconds = hours * 3600 + minutes * 60 + seconds

            meta.append({
                "video_id": item["id"],
                "title": snippet.get("title", ""),
                "published": snippet.get("publishedAt", ""),
                "duration_seconds": duration_seconds,
                "views": int(stats.get("viewCount", 0))
            })

    return meta


def main():
    ensure_longform_tables()

    youtube = build("youtube", "v3", developerKey=API_KEY)

    channel_id = get_mormonstories_channel_id(youtube)
    uploads_playlist = get_uploads_playlist_id(youtube, channel_id)

    video_ids = get_playlist_video_ids(youtube, uploads_playlist)
    print(f"Found {len(video_ids)} uploaded videos.")

    all_meta = fetch_video_metadata_batch(youtube, video_ids)
    all_meta_sorted = sorted(all_meta, key=lambda m: m["views"], reverse=True)
    top_videos = all_meta_sorted[:MAX_VIDEOS]

    model = whisper.load_model(WHISPER_MODEL)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    for i, meta in enumerate(top_videos, start=1):
        vid = meta["video_id"]
        title = meta["title"]
        pub = meta["published"]
        dur = meta["duration_seconds"]
        views = meta["views"]

        pct = (i / len(top_videos)) * 100
        print(f"[{i}/{len(top_videos)}] ({pct:.2f}%) {vid} | {views} views")

        cur.execute("""
            INSERT OR IGNORE INTO longform_videos (video_id, title, published_at, duration)
            VALUES (?, ?, ?, ?)
        """, (vid, title, pub, int(dur)))
        conn.commit()

        cur.execute("SELECT 1 FROM longform_transcripts WHERE video_id = ? LIMIT 1", (vid,))
        if cur.fetchone():
            print("Transcript already exists — skipping.")
            continue

        audio_path = download_audio(vid)
        if not audio_path:
            print("Audio unavailable — skipping.")
            continue

        try:
            result = model.transcribe(audio_path, fp16=False)
        except Exception as e:
            print(f"Transcription failed for {vid}: {e}")
            continue

        segments = result.get("segments", [])
        if not segments:
            continue

        for seg in segments:
            cur.execute("""
                INSERT INTO longform_transcripts (video_id, start, end, text)
                VALUES (?, ?, ?, ?)
            """, (vid, float(seg["start"]), float(seg["end"]), seg["text"]))

        conn.commit()
        print(f"Inserted {len(segments)} segments for {vid}.")

        time.sleep(random.uniform(1.5, 2.5))

    conn.close()
    print("Ingestion complete.")


if __name__ == "__main__":
    main()