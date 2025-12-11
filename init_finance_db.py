import sqlite3
import os

DB_PATH = os.path.abspath(
    "/Volumes/SS 1TB/LMID_ex/Analysis/Python Scripts/Church Finances/db/finance.db"
)

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# Filing index
cur.execute("""
CREATE TABLE IF NOT EXISTS sec_filings (
    accession TEXT PRIMARY KEY,
    cik TEXT,
    filing_date TEXT,
    period_of_report TEXT,
    form_type TEXT,
    raw_xml TEXT,
    retrieved_at TEXT
);
""")

# Holdings table
cur.execute("""
CREATE TABLE IF NOT EXISTS sec_holdings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    accession TEXT,
    ticker TEXT,
    cusip TEXT,
    issuer_name TEXT,
    shares INTEGER,
    value INTEGER,
    FOREIGN KEY(accession) REFERENCES sec_filings(accession)
);
""")

conn.commit()
conn.close()

print("Finance database initialized at:", DB_PATH)