import os
import sqlite3
import requests
from datetime import datetime
from bs4 import BeautifulSoup

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "db", "finance.db")
XML_DIR = os.path.join(BASE_DIR, "xml")
os.makedirs(XML_DIR, exist_ok=True)

SEC_HEADERS = {
    "User-Agent": "ResearchClient/1.0 (contact@example.com)",
    "Accept-Encoding": "gzip, deflate",
    "Host": "www.sec.gov",
}

XML_CANDIDATES = [
    "form13fInfoTable.xml",
    "infotable.xml",
    "form13fInformationTable.xml",
    "primary_doc.xml",
]

CIK = "1454984"


def get_filings_needing_xml():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        """
        SELECT accession
        FROM sec_filings
        WHERE xml_saved IS NULL OR xml_saved = 0
        ORDER BY filing_date DESC;
        """
    )
    rows = cur.fetchall()
    conn.close()
    return [r[0] for r in rows]


def mark_xml_saved(accession, path):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        """
        UPDATE sec_filings
        SET xml_saved = 1,
            xml_path = ?,
            downloaded_at = ?
        WHERE accession = ?;
        """,
        (path, datetime.utcnow().isoformat(), accession),
    )

    conn.commit()
    conn.close()


def discover_xml_url(accession):
    base = f"https://www.sec.gov/Archives/edgar/data/{CIK}/{accession}"

    for fname in XML_CANDIDATES:
        url = f"{base}/{fname}"
        r = requests.get(url, headers=SEC_HEADERS)
        if r.status_code == 200:
            return url

    index_url = f"{base}/{accession.replace('-', '')}-index.html"
    r = requests.get(index_url, headers=SEC_HEADERS)
    if r.status_code != 200:
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().endswith(".xml"):
            href = href.lstrip("/")
            return f"https://www.sec.gov/{href}"

    return None


def download_xml(accession, url):
    r = requests.get(url, headers=SEC_HEADERS)
    if r.status_code != 200:
        return None

    fname = f"{accession}.xml"
    path = os.path.join(XML_DIR, fname)

    with open(path, "wb") as f:
        f.write(r.content)

    return path


def main():
    filings = get_filings_needing_xml()
    print(f"{len(filings)} filings need XML.\n")

    for i, accession in enumerate(filings, start=1):
        print(f"[{i}/{len(filings)}] {accession}")

        xml_url = discover_xml_url(accession)
        if not xml_url:
            print("  No XML found.\n")
            continue

        print(f"  URL: {xml_url}")

        path = download_xml(accession, xml_url)
        if not path:
            print("  Download failed.\n")
            continue

        mark_xml_saved(accession, path)
        print(f"  Saved: {path}\n")

    print("XML retrieval complete.")


if __name__ == "__main__":
    main()