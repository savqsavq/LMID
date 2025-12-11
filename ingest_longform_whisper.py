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

AUDIO_DIR = os.path.join(os.path.dirname(__file__), "audio")
os.makedirs(AUDIO_DIR, exist_ok=True)

API_KEY = "REDACTED"
CHANNEL_ID = "UCqrqFLPh4cRM1VomUzG24sQ"

DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "lmid.db"))


def get_uploads_playlist_id(youtube, channel_id):
    resp = youtube.channels().list(
        part="contentDetails",
        id=channel_id
    ).execute()

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

        for item in resp["items"]:
            ids.append(item["contentDetails"]["videoId"])

        next_page = resp.get("nextPageToken")
        if not next_page:
            break

    return ids


def get_metadata(youtube, video_id):
    resp = youtube.videos().list(
        part="snippet,contentDetails",
        id=video_id
    ).execute()

    if not resp["items"]:
        return None, None, None

    item = resp["items"][0]
    title = item["snippet"]["title"]
    published = item["snippet"]["publishedAt"]
    duration_iso = item["contentDetails"]["duration"]

    hours = minutes = seconds = 0
    tmp = duration_iso.replace("PT", "")
    number = ""

    for char in tmp:
        if char.isdigit():
            number += char
        else:
            if char == "H":
                hours = int(number)
            elif char == "M":
                minutes = int(number)
            elif char == "S":
                seconds = int(number)
            number = ""

    duration_seconds = hours * 3600 + minutes * 60 + seconds
    return title, published, duration_seconds


def main():
    youtube = build("youtube", "v3", developerKey=API_KEY)

    print("Loading Whisper model...")
    model = whisper.load_model("tiny")

    uploads_playlist_id = get_uploads_playlist_id(youtube, CHANNEL_ID)
    print("Uploads playlist:", uploads_playlist_id)

    video_ids = get_playlist_video_ids(youtube, uploads_playlist_id)
    print("Found", len(video_ids), "videos")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    longform = []
    print("Filtering by duration...")

    for vid in video_ids:
        title, pub, dur = get_metadata(youtube, vid)
        if dur and dur >= 300:
            longform.append((vid, title, pub, dur))

    print("Long-form videos:", len(longform))

    for i, (vid, title, pub, dur) in enumerate(longform, start=1):
        pct = (i / len(longform)) * 100
        print(f"[{i}/{len(longform)}] ({pct:.2f}%) {vid} - {title}")

        cur.execute("""
            INSERT OR IGNORE INTO longform_videos (video_id, title, published_at, duration)
            VALUES (?, ?, ?, ?)
        """, (vid, title, pub, dur))
        conn.commit()

        audio_path = download_audio(vid)
        if not audio_path or not os.path.exists(audio_path):
            print("Failed to download audio -> skipping")
            continue

        try:
            result = model.transcribe(audio_path, fp16=False)
        except Exception as e:
            print(f"Whisper error for {vid}: {e}")
            continue

        for seg in result.get("segments", []):
            start = float(seg["start"])
            end = float(seg["end"])
            text = seg["text"]

            cur.execute("""
                INSERT INTO longform_transcripts (video_id, start, end, text)
                VALUES (?, ?, ?, ?)
            """, (vid, start, end, text))

        conn.commit()
        time.sleep(random.uniform(1.5, 2.8))

    conn.close()
    print("DONE — Whisper long-form ingestion complete.")


if __name__ == "__main__":
    main()