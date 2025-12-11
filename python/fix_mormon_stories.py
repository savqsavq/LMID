import sqlite3

DB = "/Volumes/SS 1TB/LMID_ex/Analysis/Everything SQL db/lmid_master.db"


def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    cur.execute("""
        SELECT id
        FROM documents
        WHERE domain = 'mormon_stories'
    """)
    rows = cur.fetchall()

    print(f"Found {len(rows)} rows to update from legacy mormon_stories domain.")

    for (doc_id,) in rows:
        cur.execute(
            """
            UPDATE documents
            SET domain = 'ex_member',
                subdomain = 'mormon_stories'
            WHERE id = ?
            """,
            (doc_id,),
        )

    conn.commit()
    conn.close()
    print("Domain normalization complete.")


if __name__ == "__main__":
    main()