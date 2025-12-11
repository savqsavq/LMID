import requests
from bs4 import BeautifulSoup
import re
import unicodedata
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import sqlite3
from datetime import datetime

BASE = "https://www.churchofjesuschrist.org"


# Basic fetch helper
def get_soup(url):
    try:
        r = requests.get(url, allow_redirects=True, timeout=10)
        r.raise_for_status()
        return BeautifulSoup(r.content, "html5lib")
    except Exception as e:
        print(f"Fetch error {url}: {e}")
        return None


# Conference index URLs (1971–2024)
def build_conference_urls(start_year=1971, end_year=2024):
    urls = []
    for year in range(start_year, end_year + 1):
        for month in ("04", "10"):
            urls.append(f"{BASE}/study/general-conference/{year}/{month}?lang=eng")
    return urls


# Collect individual talk URLs from a conference page
def scrape_talk_urls(conference_url):
    soup = get_soup(conference_url)
    if soup is None:
        return []

    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r"/study/general-conference/\d{4}/(04|10)/", href):
            full = href if href.startswith("http") else BASE + href
            links.add(full.split("?")[0])

    return list(links)


# Scrape a single talk page
def scrape_talk_data(url):
    soup = get_soup(url)
    if soup is None:
        return {}

    title = soup.find("h1")
    speaker = soup.find("p", class_="author-name")
    calling = soup.find("p", class_="author-role")
    conference_tag = soup.find("p", class_="subtitle")

    content_blocks = [
        ("div", {"class": "body-block"}),
        ("div", {"class": "article-body"}),
        ("section", {"class": "body-content"}),
        ("div", {"id": "primary"}),
    ]

    body = None
    for tag, attrs in content_blocks:
        body = soup.find(tag, attrs)
        if body:
            break

    paragraphs = (
        [p.get_text(" ", strip=True) for p in body.find_all("p")]
        if body else []
    )
    content = "\n\n".join(paragraphs)

    notes = soup.find_all("li", class_="study-note")
    footnotes = "\n".join(
        f"{i+1}. {n.get_text(strip=True)}" for i, n in enumerate(notes)
    ) if notes else ""

    year_match = re.search(r"/(\d{4})/", url)
    year = year_match.group(1) if year_match else ""

    season = "April" if "/04/" in url else "October"

    return {
        "title": (title.text.strip() if title else ""),
        "speaker": (speaker.text.strip() if speaker else ""),
        "calling": (calling.text.strip() if calling else ""),
        "year": year,
        "season": season,
        "conference": (conference_tag.text.strip() if conference_tag else ""),
        "url": url,
        "talk": content,
        "footnotes": footnotes,
    }


# Threaded scrape wrapper
def scrape_talk_data_parallel(urls):
    if not urls:
        return []
    with ThreadPoolExecutor(max_workers=10) as ex:
        results = list(
            tqdm(ex.map(scrape_talk_data, urls), total=len(urls), desc="Scraping")
        )
    return [r for r in results if r]


# Insert scraped records into SQLite
def save_to_db(records):
    if not records:
        return

    conn = sqlite3.connect("gc.db")
    cur = conn.cursor()

    for rec in records:
        for k, v in rec.items():
            if isinstance(v, str):
                rec[k] = unicodedata.normalize("NFD", v).replace("\t", "")

        cur.execute(
            """
            INSERT INTO talks (year, month, title, speaker, calling, url, text, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                rec["year"],
                rec["season"],
                rec["title"],
                rec["speaker"],
                rec["calling"],
                rec["url"],
                rec["talk"],
                datetime.now().isoformat(),
            ),
        )

    conn.commit()
    conn.close()
    print(f"{len(records)} talks inserted.")


# Pipeline
def main_scrape_process():
    conf_urls = build_conference_urls(1971, 2024)

    all_talks = set()
    for url in tqdm(conf_urls, desc="Collecting URLs"):
        for t in scrape_talk_urls(url):
            all_talks.add(t)

    all_talks = list(all_talks)
    print(f"Total unique talks: {len(all_talks)}")

    if not all_talks:
        print("No talks detected.")
        return

    records = scrape_talk_data_parallel(all_talks)
    save_to_db(records)

    print("Scraping complete.")


if __name__ == "__main__":
    start = time.time()
    main_scrape_process()
    print(f"Time: {time.time() - start} seconds")