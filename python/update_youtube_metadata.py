import sqlite3
import json

DB_PATH = "/Volumes/SS 1TB/LMID_ex/Analysis/Everything SQL db/lmid_master.db"


def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Load metadata to apply
    cur.execute("""
        SELECT doc_id, channel_id, channel_name
        FROM tmp_youtube_meta
    """)
    updates = cur.fetchall()

    # Fetch existing JSON blobs for all rows to modify
    cur.execute("""
        SELECT id, extra
        FROM documents
        WHERE id IN (SELECT doc_id FROM tmp_youtube_meta)
    """)
    existing = {doc_id: extra for doc_id, extra in cur.fetchall()}

    total = 0

    for doc_id, channel_id, channel_name in updates:
        raw = existing.get(doc_id)
        if raw is None:
            continue

        try:
            extra = json.loads(raw) if raw else {}
        except Exception:
            extra = {}

        extra["channel_id"] = channel_id
        extra["channel_name"] = channel_name

        cur.execute(
            "UPDATE documents SET extra = ? WHERE id = ?",
            (json.dumps(extra), doc_id)
        )

        total += 1
        if total % 50000 == 0:
            conn.commit()

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()