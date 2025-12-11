import os
import sqlite3
import time
import random
from googleapiclient.discovery import build

API_KEY = "REDACTED"

DB_PATH = os.path.abspath(
    "/Volumes/SS 1TB/LMID_ex/Analysis/Everything SQL db/lmid.db"
)

BATCH = 50
SLEEP = (0.15, 0.35)


def fetch_channel_info(youtube, video_ids):
    """Return a dict {video_id: (channel_id, channel_title)}."""
    mapping = {}

    resp = youtube.videos().list(
        part="snippet",
        id=",".join(video_ids),
        maxResults=len(video_ids)
    ).execute()

    for item in resp.get("items", []):
        vid = item["id"]
        snippet = item.get("snippet", {})
        mapping[vid] = (
            snippet.get("channelId"),
            snippet.get("channelTitle")
        )

    return mapping


def main():
    youtube = build("youtube", "v3", developerKey=API_KEY)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("SELECT video_id FROM longform_videos")
    all_ids = [row[0] for row in cur.fetchall()]
    total = len(all_ids)

    print(f"Total videos to update: {total}")

    updated = 0

    for i in range(0, total, BATCH):
        chunk = all_ids[i:i + BATCH]

        pct = (i / total) * 100
        print(f"[{i}/{total}] {pct:.1f}% — fetching metadata")

        mapping = fetch_channel_info(youtube, chunk)

        for vid in chunk:
            if vid not in mapping:
                continue

            channel_id, channel_title = mapping[vid]

            cur.execute(
                """
                UPDATE longform_videos
                SET channel_id = ?, channel_name = ?
                WHERE video_id = ?
                """,
                (channel_id, channel_title, vid),
            )

            updated += 1

        conn.commit()
        time.sleep(random.uniform(*SLEEP))

    conn.close()
    print(f"\nDONE — Updated {updated} rows.")
    print("Channel metadata refresh complete.")


if __name__ == "__main__":
    main()