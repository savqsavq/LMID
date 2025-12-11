import sqlite3

conn = sqlite3.connect("gc.db")
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS talks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    year TEXT,
    month TEXT,
    title TEXT,
    speaker TEXT,
    calling TEXT,
    url TEXT UNIQUE,
    text TEXT,
    scraped_at TEXT
);
""")

conn.commit()
conn.close()

print("Initialized")