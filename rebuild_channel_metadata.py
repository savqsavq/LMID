import sqlite3
import time
import random
from googleapiclient.discovery import build

API_KEY = "AIzaSyA5pIw_X_R6BRU4XyFkqm90j88yJhpIKM4"
DB_PATH = "/Volumes/SS 1TB/LMID_ex/Analysis/Everything SQL db/lmid.db"


def main():
    youtube = build("youtube", "v3", developerKey=API_KEY)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        SELECT video_id
        FROM longform_videos
        WHERE channel_id IS NULL
    """)
    vids = [r[0] for r in cur.fetchall()]
    print(f"{len(vids)} videos missing channel metadata.")

    for i, vid in enumerate(vids, start=1):
        print(f"[{i}/{len(vids)}] Fetching metadata for {vid}")

        try:
            resp = youtube.videos().list(
                part="snippet",
                id=vid
            ).execute()

            items = resp.get("items", [])
            if not items:
                continue

            snip = items[0]["snippet"]
            chan_id = snip.get("channelId")
            chan_title = snip.get("channelTitle")

            cur.execute("""
                UPDATE longform_videos
                SET channel_id = ?, channel_name = ?
                WHERE video_id = ?
            """, (chan_id, chan_title, vid))
            conn.commit()

        except Exception as e:
            print("Error:", e)
            continue

        time.sleep(random.uniform(0.3, 0.6))

    conn.close()
    print("Metadata update complete.")


if __name__ == "__main__":
    main()