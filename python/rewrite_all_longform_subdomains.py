import sqlite3
import json
import re

MASTER_DB = "/Volumes/SS 1TB/LMID_ex/Analysis/Everything SQL db/lmid_master.db"
SRC_DB    = "/Volumes/SS 1TB/LMID_ex/Analysis/Everything SQL db/lmid.db"


def normalize(name):
    name = name.lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")


def main():
    conn = sqlite3.connect(MASTER_DB)
    conn.execute(f"ATTACH DATABASE '{SRC_DB}' AS src;")
    cur = conn.cursor()

    cur.execute("""
        SELECT video_id, channel_name
        FROM src.longform_videos
        WHERE channel_name IS NOT NULL
    """)
    items = cur.fetchall()

    mapping = {vid: normalize(ch) for vid, ch in items}

    updated = 0
    for vid, subdomain in mapping.items():
        cur.execute("""
            UPDATE documents
            SET subdomain = ?
            WHERE domain = 'youtube_longform'
              AND json_extract(extra, '$.video_id') = ?
        """, (subdomain, vid))
        updated += cur.rowcount

    cur.execute("INSERT INTO documents(documents) VALUES ('rebuild');")

    conn.commit()
    conn.close()

    print(f"Updated {updated} rows.")


if __name__ == "__main__":
    main()