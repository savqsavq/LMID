import sqlite3

DB = "/Volumes/SS 1TB/LMID_ex/Analysis/Everything SQL db/lmid_master.db"


def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    # General Conference → church/general_conference
    cur.execute(
        "SELECT id FROM documents WHERE domain = 'lds_conference';"
    )
    gc_rows = cur.fetchall()
    print(f"General conference records: {len(gc_rows)}")

    for (doc_id,) in gc_rows:
        cur.execute(
            """
            UPDATE documents
            SET domain = 'church',
                subdomain = 'general_conference'
            WHERE id = ?
            """,
            (doc_id,),
        )

    # Church literature → church/literature
    cur.execute(
        "SELECT id FROM documents WHERE domain = 'church_literature';"
    )
    lit_rows = cur.fetchall()
    print(f"Church literature records: {len(lit_rows)}")

    for (doc_id,) in lit_rows:
        cur.execute(
            """
            UPDATE documents
            SET domain = 'church',
                subdomain = 'literature'
            WHERE id = ?
            """,
            (doc_id,),
        )

    conn.commit()
    conn.close()
    print("Church domain normalization done")


if __name__ == "__main__":
    main()