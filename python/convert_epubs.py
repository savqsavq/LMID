import os
import subprocess
import tempfile
import PyPDF2
from ebooklib import epub, ITEM_DOCUMENT
from bs4 import BeautifulSoup
import docx

# Output location
OUTPUT_DIR = "txt_output"
CALIBRE_PATH = "/opt/homebrew/bin/ebook-convert"   # Adjust if needed

os.makedirs(OUTPUT_DIR, exist_ok=True)


# EPUB
def convert_epub(path):
    try:
        book = epub.read_epub(path)
        chunks = []
        for item in book.get_items():
            if item.get_type() == ITEM_DOCUMENT:
                soup = BeautifulSoup(item.get_content(), "html.parser")
                chunks.append(soup.get_text())
        return "\n\n".join(chunks)
    except Exception as e:
        print(f"[EPUB error] {path}: {e}")
        return ""


# PDF
def convert_pdf(path):
    try:
        out = []
        with open(path, "rb") as f:
            reader = PyPDF2.PdfReader(f)
            for p in reader.pages:
                text = p.extract_text()
                if text:
                    out.append(text)
        return "\n\n".join(out)
    except Exception as e:
        print(f"[PDF error] {path}: {e}")
        return ""


# DOCX
def convert_docx(path):
    try:
        doc = docx.Document(path)
        return "\n".join(p.text for p in doc.paragraphs)
    except Exception as e:
        print(f"[DOCX error] {path}: {e}")
        return ""


# Plain text / markdown
def convert_plain(path):
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()
    except Exception as e:
        print(f"[TXT/MD error] {path}: {e}")
        return ""


# HTML
def convert_html(path):
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            soup = BeautifulSoup(f.read(), "html.parser")
            return soup.get_text()
    except Exception as e:
        print(f"[HTML error] {path}: {e}")
        return ""


# Calibre (for AZW3/MOBI/FB2/KFX)
def convert_with_calibre(path):
    try:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".txt")
        tmp.close()

        subprocess.run(
            [CALIBRE_PATH, path, tmp.name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )

        with open(tmp.name, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()
    except Exception as e:
        print(f"[Calibre error] {path}: {e}")
        return ""


# Dispatcher
def convert_file(path):
    ext = os.path.splitext(path)[1].lower()

    if ext == ".epub":
        return convert_epub(path)
    if ext == ".pdf":
        return convert_pdf(path)
    if ext == ".docx":
        return convert_docx(path)
    if ext in {".txt", ".md"}:
        return convert_plain(path)
    if ext in {".html", ".htm"}:
        return convert_html(path)
    if ext in {".azw3", ".mobi", ".fb2", ".kfx"}:
        return convert_with_calibre(path)
    return None


# Batch driver
def batch_convert(directory="."):
    for fname in os.listdir(directory):
        full = os.path.join(directory, fname)
        if os.path.isdir(full):
            continue

        text = convert_file(full)
        if text is None:
            print(f"Skipped: {fname}")
            continue

        out_name = fname.rsplit(".", 1)[0] + ".txt"
        out_path = os.path.join(OUTPUT_DIR, out_name)

        with open(out_path, "w", encoding="utf-8") as f:
            f.write(text)

        print(f"Converted: {fname} → {out_path}")


if __name__ == "__main__":
    batch_convert()