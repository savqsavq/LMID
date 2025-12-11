import sqlite3

GC_DB = "/Volumes/SS 1TB/LMID_ex/Analysis/Python Scripts/2. Church analysis/General Conference Scrape/gc.db"
MASTER_DB = "lmid_master.db"

SEGMENT_SIZE = 420


def segment_text(text, size=SEGMENT_SIZE):
    words = text.split()
    for i in range(0, len(words), size):
        yield " ".join(words[i:i+size])


gc_conn = sqlite3.connect(GC_DB)
gc_cur = gc_conn.cursor()

master_conn = sqlite3.connect(MASTER_DB)
master_cur = master_conn.cursor()

gc_cur.execute("SELECT id, title, speaker, text FROM talks")
rows = gc_cur.fetchall()

count_segments = 0

for talk_id, title, speaker, body in rows:
    if not body:
        continue

    for segment in segment_text(body):
        master_cur.execute(
            """
            INSERT INTO documents (domain, source_type, title, author, text)
            VALUES (?, ?, ?, ?, ?)
            """,
            ("lds_conference", "gc_db", title, speaker, segment),
        )
        count_segments += 1

master_conn.commit()
gc_conn.close()
master_conn.close()

print("Inserted done", count_segments, "GC segments.")