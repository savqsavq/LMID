import sqlite3

DB_PATH = "/Volumes/SS 1TB/LMID_ex/Analysis/Everything SQL db/lmid_master.db"


def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Normalize Alyssa
    cur.execute(
        """
        UPDATE documents
        SET domain = 'ex_member',
            subdomain = 'alyssa'
        WHERE domain = 'creator_exmormon'
          AND subdomain = 'alyssa';
        """
    )
    alyssa_count = cur.rowcount
    print(f"Alyssa reassigned: {alyssa_count}")

    # Normalize Jonny
    cur.execute(
        """
        UPDATE documents
        SET domain = 'ex_member',
            subdomain = 'jonny'
        WHERE domain = 'creator_exmormon'
          AND subdomain = 'jonny';
        """
    )
    jonny_count = cur.rowcount
    print(f"Jonny reassigned: {jonny_count}")

    conn.commit()

    # Summary
    print("\nUpdated ex_member breakdown:")
    cur.execute(
        """
        SELECT subdomain, COUNT(*)
        FROM documents
        WHERE domain = 'ex_member'
        GROUP BY subdomain
        ORDER BY subdomain;
        """
    )
    for row in cur.fetchall():
        print(row)

    conn.close()


if __name__ == "__main__":
    main()