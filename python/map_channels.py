import sqlite3
import csv
from googleapiclient.discovery import build

API_KEY = "REDACTED"
DB_PATH = "/Volumes/SS 1TB/LMID_ex/Analysis/Everything SQL db/lmid_master.db"

youtube = build("youtube", "v3", developerKey=API_KEY)

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

rows = cur.execute("""
    SELECT DISTINCT json_extract(extra, '$.video_id')
    FROM documents
    WHERE subdomain = 'mormon_stories'
      AND json_extract(extra, '$.video_id') IS NOT NULL
""").fetchall()

video_ids = [r[0] for r in rows]
print(f"{len(video_ids)} video IDs loaded.")

results = []

for i in range(0, len(video_ids), 50):
    chunk = video_ids[i:i+50]

    resp = youtube.videos().list(
        part="snippet",
        id=",".join(chunk)
    ).execute()

    for item in resp.get("items", []):
        vid = item["id"]
        snip = item["snippet"]
        results.append((
            vid,
            snip.get("channelId"),
            snip.get("channelTitle")
        ))

with open("video_channel_map.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["video_id", "channel_id", "channel_title"])
    w.writerows(results)

print("video_channel_map.csv written.")