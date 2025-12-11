import sqlite3
import json

DB = "/Volumes/SS 1TB/LMID_ex/Analysis/Everything SQL db/lmid_master.db"

CHANNEL_MAP = {
    "Mormon Stories Podcast": ("ex_member", "mormon_stories"),
    "Don't Miss This": ("active_member", "dont_miss_this"),
    "Saints Unscripted": ("active_member", "saints_unscripted"),
    "LDS Living": ("active_member", "lds_living"),
}


def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    cur.execute("""
        SELECT id, extra
        FROM documents
        WHERE source_type = 'yt_segment'
    """)

    rows = cur.fetchall()
    total = len(rows)
    print(f"Found {total} YouTube segments.")

    updated = 0

    for idx, (doc_id, extra_json) in enumerate(rows):
        try:
            extra = json.loads(extra_json)
        except Exception:
            continue

        channel_name = extra.get("channel_name")
        if not channel_name:
            continue

        if channel_name in CHANNEL_MAP:
            domain, subdomain = CHANNEL_MAP[channel_name]
            cur.execute(
                "UPDATE documents SET domain = ?, subdomain = ? WHERE id = ?",
                (domain, subdomain, doc_id)
            )
            updated += 1

        if idx % 50000 == 0 and idx > 0:
            print(f"{idx}/{total} processed")

    conn.commit()
    conn.close()

    print(f"Updated rows: {updated}")


if __name__ == "__main__":
    main()