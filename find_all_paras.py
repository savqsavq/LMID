from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

URL = "https://speeches.byu.edu/talks/b-corey-cuvelier/what-desirest-thou/"

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()

    page.goto(URL)
    page.wait_for_load_state("networkidle")

    html = page.content()
    soup = BeautifulSoup(html, "html.parser")

    paras = soup.find_all("p")
    print("Total <p> tags:", len(paras))

    for i, ptag in enumerate(paras[:40]):
        print(f"{i:03d}:", ptag.get_text(strip=True)[:120])
