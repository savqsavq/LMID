import sqlite3

DB = "/Volumes/SS 1TB/LMID_ex/Analysis/Everything SQL db/lmid_master.db"


def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    cur.execute(
        "SELECT id FROM documents WHERE domain = 'active_members';"
    )
    rows = cur.fetchall()
    print(f"Rows to update: {len(rows)}")

    for (doc_id,) in rows:
        cur.execute(
            """
            UPDATE documents
            SET domain = 'active_member'
            WHERE id = ?
            """,
            (doc_id,),
        )

    conn.commit()
    conn.close()
    print("Domain normalization complete.")


if __name__ == "__main__":
    main()