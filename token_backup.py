import sqlite3
import re
from collections import Counter
from pathlib import Path

MASTER_DB = Path("/Volumes/SS 1TB/LMID_ex/Analysis/Everything SQL db/lmid_master.db")


def tokenize(text):
    """Normalize and split text into tokens."""
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s']", " ", text)
    return text.split()


def load_stopwords(conn):
    """Load stopwords from the database."""
    cur = conn.cursor()
    cur.execute("SELECT word FROM stopwords")
    return {row[0].strip().lower() for row in cur.fetchall()}


def bigrams(tokens):
    """Yield adjacent token pairs."""
    for i in range(len(tokens) - 1):
        yield (tokens[i], tokens[i + 1])


def main():
    conn = sqlite3.connect(MASTER_DB)
    cur = conn.cursor()

    stopwords = load_stopwords(conn)
    unigram_counts = Counter()
    bigram_counts = Counter()

    cur.execute("""
        SELECT text
        FROM documents
        WHERE domain = 'youtube_longform'
    """)

    for (text,) in cur:
        tokens = [t for t in tokenize(text) if t not in stopwords]
        unigram_counts.update(tokens)
        bigram_counts.update(bigrams(tokens))

    # Create output tables if needed
    cur.execute("""
        CREATE TABLE IF NOT EXISTS unigram_counts (
            token TEXT PRIMARY KEY,
            count INTEGER
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS bigram_counts (
            token1 TEXT,
            token2 TEXT,
            count INTEGER,
            PRIMARY KEY (token1, token2)
        )
    """)

    # Clear old data
    cur.execute("DELETE FROM unigram_counts")
    cur.execute("DELETE FROM bigram_counts")

    # Insert results
    cur.executemany(
        "INSERT INTO unigram_counts (token, count) VALUES (?, ?)",
        unigram_counts.items()
    )

    cur.executemany(
        "INSERT INTO bigram_counts (token1, token2, count) VALUES (?, ?, ?)",
        [(w1, w2, n) for (w1, w2), n in bigram_counts.items()]
    )

    conn.commit()
    conn.close()

    print("DONE: counts written to SQLite.")
    print(f"Unigrams: {len(unigram_counts)}")
    print(f"Bigrams: {len(bigram_counts)}")


if __name__ == "__main__":
    main()