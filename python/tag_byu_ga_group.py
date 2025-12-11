import sqlite3
import os
from pathlib import Path

DB_PATH = Path("/Volumes/SS 1TB/LMID_ex/Analysis/Everything SQL db/lmid_master.db")


def ensure_column(conn):
    """Add byu_ga_group column if not already present."""
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(documents);")
    cols = [row[1] for row in cur.fetchall()]

    if "byu_ga_group" not in cols:
        cur.execute("ALTER TABLE documents ADD COLUMN byu_ga_group TEXT;")
        conn.commit()


def load_ga_names(conn):
    """Return a set of all GA speaker names."""
    cur = conn.cursor()
    cur.execute("SELECT speaker FROM ga_names;")
    names = {row[0] for row in cur.fetchall()}
    print(f"Loaded {len(names)} GA names.")
    return names


def main():
    if not DB_PATH.exists():
        raise SystemExit(f"Database not found: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    ensure_column(conn)
    ga_names = load_ga_names(conn)

    # Fetch speakers for all BYU talk segments.
    cur.execute("""
        SELECT id, json_extract(extra, '$.speaker') AS speaker
        FROM documents
        WHERE domain = 'active_members'
          AND source_type = 'byu_talks_segment_420w'
    """)
    rows = cur.fetchall()
    total = len(rows)
    print(f"Found {total} BYU segments to tag.")

    ga_count = 0
    non_ga_count = 0

    for idx, row in enumerate(rows, start=1):
        doc_id = row["id"]
        speaker = row["speaker"]

        tag = "ga" if speaker in ga_names else "non_ga"

        if tag == "ga":
            ga_count += 1
        else:
            non_ga_count += 1

        cur.execute(
            "UPDATE documents SET byu_ga_group = ? WHERE id = ?;",
            (tag, doc_id)
        )

        if idx % 1000 == 0:
            conn.commit()
            print(f"Tagged {idx}/{total} segments…")

    conn.commit()
    conn.close()

    print("Classification complete.")
    print(f"GA segments:     {ga_count}")
    print(f"Non-GA segments: {non_ga_count}")


if __name__ == "__main__":
    main()