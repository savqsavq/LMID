import sqlite3
import os

DB_PATH = "/Volumes/SS 1TB/LMID_ex/Analysis/Everything SQL db/lmid.db"

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# Video metadata table
cur.execute("""
CREATE TABLE IF NOT EXISTS longform_videos (
    video_id TEXT PRIMARY KEY,
    title TEXT,
    published_at TEXT,
    duration INTEGER
);
""")

# Transcript segments
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

print("Verified: longform_videos and longform_transcripts tables exist in lmid.db.")