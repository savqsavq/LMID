from playwright.sync_api import sync_playwright
import requests, sqlite3, time, re
from datetime import datetime
from bs4 import BeautifulSoup

# basic config for API + DB
API_BASE = "https://speeches.byu.edu/wp-json/wp/v2/speech"
DB_PATH = "BYU.db"
HEADERS = {"User-Agent": "Mozilla/5.0"}

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS byu_talks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT,
    speaker TEXT,
    event_type TEXT,
    topics TEXT,
    date TEXT,
    url TEXT UNIQUE,
    transcript TEXT,
    created_at TEXT
)
""")
con.commit()

def api_page(page):
    r = requests.get(f"{API_BASE}?page={page}&per_page=100", headers=HEADERS, timeout=30)
    if r.status_code == 400:
        return []
    r.raise_for_status()
    return r.json()

# helpers 
def parse_speaker(classes):
    for c in classes:
        if c.startswith("speaker-"):
            return c.replace("speaker-", "").replace("-", " ").title()
    return None

def parse_event_type(classes):
    for c in classes:
        if c.startswith("event_type-"):
            return c.replace("event_type-", "")
    return None

def parse_topics(classes):
    return ", ".join(
        c.replace("topic-", "").replace("-", " ").title()
        for c in classes if c.startswith("topic-")
    )

def extract_transcript(page):
    html = page.content()
    soup = BeautifulSoup(html, "html.parser")

    paragraphs = []

    for p in soup.select("main p"):
        txt = p.get_text(" ", strip=True)
        if txt and is_real_paragraph(txt):
            paragraphs.append(txt)

    if len(paragraphs) > 5:
        return clean_text(" ".join(paragraphs))

    # older layout patterns
    fallbacks = [
        "div.talk-content__body p",
        "div.sp-wrapper-content p",
        "div.wp-block-sp-content p",
    ]

    for sel in fallbacks:
        for p in soup.select(sel):
            txt = p.get_text(" ", strip=True)
            if txt and is_real_paragraph(txt):
                paragraphs.append(txt)
        if paragraphs:
            break

    return clean_text(" ".join(paragraphs))

def is_real_paragraph(text):
    t = text.strip()
    if len(t.split()) < 6:
        return False

    low = t.lower()

    noise = [
        "your browser does not support",
        "home > speeches",
        "full video",
        "highlight video",
        "see also",
        "sign up for the byu speeches",
    ]

    if low in {"english", "contact"}:
        return False
    if low.startswith(("speed", "0:00", "audio")):
        return False
    if any(n in low for n in noise):
        return False
    if "copyright" in low:
        return False

    return True

def clean_text(t):
    return re.sub(r"\s+", " ", t).strip()


print("Starting BYU scrape...")

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page_obj = browser.new_page()

    total_inserted = 0

    # figure out roughly how many pages exist
    print("Determining API page range...")
    first_page = api_page(1)
    if not first_page:
        raise SystemExit("API returned nothing.")

    max_page = 1
    for guess in [20, 40, 60, 80, 100, 150, 200]:
        if api_page(guess):
            max_page = guess
        else:
            break

    print(f"Max API page estimate: {max_page}")

    for page_num in range(max_page, 0, -1):
        print(f"\nAPI page {page_num}")
        talks = api_page(page_num)

        if not talks:
            continue

        for item in talks:
            title = item["title"]["rendered"].strip()
            url = item["link"]
            date = item["date"]
            classes = item.get("class_list", [])

            speaker = parse_speaker(classes)
            event_type = parse_event_type(classes)
            topics = parse_topics(classes)

            print(f"\nChecking: {title}")

            # skip if already saved
            cur.execute("SELECT 1 FROM byu_talks WHERE url=?", (url,))
            if cur.fetchone():
                print("Already in DB.")
                continue

            # load page
            try:
                page_obj.goto(url, timeout=60000)
                page_obj.wait_for_load_state("networkidle")
            except Exception as e:
                print("Page load error:", e)
                continue

            transcript = extract_transcript(page_obj)
            if len(transcript) < 300:
                print("Transcript missing or too short. Skipping.")
                continue

            print(f"Saving transcript ({len(transcript)} chars).")

            cur.execute("""
                INSERT INTO byu_talks
                (title, speaker, event_type, topics, date, url, transcript, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                title,
                speaker,
                event_type,
                topics,
                date,
                url,
                transcript,
                datetime.utcnow().isoformat()
            ))
            con.commit()
            total_inserted += 1

            time.sleep(1.0)

    browser.close()

con.close()
print(f"\nDone. Inserted {total_inserted} talks.")