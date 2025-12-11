import os
import sqlite3
import time
import random
import whisper
from googleapiclient.discovery import build

from audio_path import download_audio

API_KEY = "REDACTED_API_KEY"

VIDEO_IDS = [
    "23owsv-R0fs",
    "hUW7j9GmXjI",
    "aTMsfOcHiJg",
    "kieCFwMqKtE"
]

DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "lmid.db"))


def main():
    youtube = build("youtube", "v3", developerKey=API_KEY)

    print("Loading Whisper model...")
    model = whisper.load_model("tiny")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    for i, vid in enumerate(VIDEO_IDS, start=1):
        print(f"[{i}/{len(VIDEO_IDS)}] Fetching metadata for {vid}")

        resp = youtube.videos().list(
            part="snippet,contentDetails",
            id=vid
        ).execute()

        if not resp.get("items"):
            print(f"Missing video: {vid}")
            continue

        item = resp["items"][0]
        title = item["snippet"]["title"]
        pub = item["snippet"]["publishedAt"]

        iso = item["contentDetails"]["duration"].replace("PT", "")
        h = m = s = 0
        num = ""

        for c in iso:
            if c.isdigit():
                num += c
            else:
                if c == "H":
                    h = int(num)
                elif c == "M":
                    m = int(num)
                elif c == "S":
                    s = int(num)
                num = ""

        dur = h * 3600 + m * 60 + s

        cur.execute("""
            INSERT OR REPLACE INTO jonny_videos (video_id, title, published_at, duration)
            VALUES (?, ?, ?, ?)
        """, (vid, title, pub, dur))
        conn.commit()

        print("Downloading audio...")
        audio_path = download_audio(vid)
        if not audio_path:
            print("Audio download failed.")
            continue

        print("Transcribing...")
        try:
            result = model.transcribe(audio_path, fp16=False)
        except Exception as e:
            print(f"Transcription failed for {vid}: {e}")
            continue

        for seg in result.get("segments", []):
            cur.execute("""
                INSERT INTO jonny_transcripts (video_id, start, end, text)
                VALUES (?, ?, ?, ?)
            """, (
                vid,
                float(seg["start"]),
                float(seg["end"]),
                seg["text"]
            ))

        conn.commit()
        time.sleep(random.uniform(1.3, 2.4))

    conn.close()
    print("DONE — Jonny ingestion complete.")


if __name__ == "__main__":
    main()