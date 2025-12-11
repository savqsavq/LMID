import sqlite3
from pathlib import Path

DB_PATH = Path("/Volumes/SS 1TB/LMID_ex/Analysis/Everything SQL db/lmid_master.db")


def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Move GA-identified BYU segments into the church domain.
    cur.execute("""
        UPDATE documents
        SET 
            domain = 'church',
            subdomain = 'byu_ga'
        WHERE source_type = 'byu_talks_segment_420w'
          AND byu_ga_group = 'ga';
    """)
    ga_count = cur.rowcount
    print(f"GA BYU talks moved to church domain: {ga_count}")

    # Move remaining (non-GA) BYU segments into the active_member domain.
    cur.execute("""
        UPDATE documents
        SET 
            domain = 'active_member',
            subdomain = 'byu_non_ga'
        WHERE source_type = 'byu_talks_segment_420w'
          AND byu_ga_group = 'non_ga';
    """)
    non_ga_count = cur.rowcount
    print(f"Non-GA BYU talks assigned to active_member domain: {non_ga_count}")

    conn.commit()

    # Summary of the final classification.
    cur.execute("""
        SELECT domain, subdomain, COUNT(*)
        FROM documents
        WHERE source_type = 'byu_talks_segment_420w'
        GROUP BY domain, subdomain
        ORDER BY domain, subdomain;
    """)

    print("\nVerification:")
    for row in cur.fetchall():
        print(row)

    conn.close()


if __name__ == "__main__":
    main()