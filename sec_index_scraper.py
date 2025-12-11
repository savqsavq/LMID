import requests
import sqlite3
import os
import time
from datetime import datetime
from pathlib import Path

CIK = "0001454984"     # Ensign Peak Advisors
BASE_URL = f"https://data.sec.gov/submissions/CIK{CIK}.json"

DB_PATH = Path(
    "/Volumes/SS 1TB/LMID_ex/Analysis/Python Scripts/Church Finances/db/finance.db"
)

HEADERS = {
    "User-Agent": "Research Project Contact (example@example.com)",
    "Accept-Encoding": "gzip, deflate",
}


def get_db():
    return sqlite3.connect(DB_PATH)


def fetch_index_json():
    """Download the SEC EDGAR submission index JSON for the given CIK."""
    print("Downloading SEC submission index…")
    r = requests.get(BASE_URL, headers=HEADERS)
    if r.status_code != 200:
        raise RuntimeError(f"Request failed with status {r.status_code}")
    return r.json()


def insert_filing(conn, filing):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO sec_filings
        (accession, cik, filing_date, period_of_report, form_type, raw_xml, retrieved_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            filing["accession"],
            CIK,
            filing["filing_date"],
            filing["period_of_report"],
            filing["form_type"],
            None,
            datetime.utcnow().isoformat(),
        ),
    )
    conn.commit()


def extract_filings(data):
    """Extract 13F filings from SEC JSON, guarding for unequal list lengths."""
    recent = data.get("filings", {}).get("recent", {})

    accessions = recent.get("accessionNumber", [])
    filing_dates = recent.get("filingDate", [])
    report_dates = recent.get("periodOfReport", [])
    form_types = recent.get("form", [])

    result = []
    n = len(accessions)

    for i in range(n):
        form = form_types[i] if i < len(form_types) else None
        if not form or not form.startswith("13F"):
            continue

        accession = (
            accessions[i].replace("-", "") if i < len(accessions) else None
        )
        filing_date = filing_dates[i] if i < len(filing_dates) else None
        report_date = report_dates[i] if i < len(report_dates) else None

        result.append(
            {
                "accession": accession,
                "filing_date": filing_date,
                "period_of_report": report_date,
                "form_type": form,
            }
        )

    return result


def main():
    print("SEC filing index scraper (Layer 1)")
    conn = get_db()

    data = fetch_index_json()
    filings = extract_filings(data)

    print(f"13F filings found: {len(filings)}")

    for f in filings:
        print(f"Inserting {f['accession']} ({f['form_type']})…")
        insert_filing(conn, f)
        time.sleep(0.2)

    conn.close()
    print("Layer 1 complete.")


if __name__ == "__main__":
    main()