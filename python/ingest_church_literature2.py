import os
import sqlite3
import json

DB_PATH = "lmid_master.db"
ROOT_DIR = "/Volumes/SS 1TB/LMID_ex/Analysis/Scrapes/Church Doctrine/txt files"

DOMAIN = "church_literature"
SOURCE_TYPE = "txt_segment_420w"
SEG_LEN = 420


def segment_text(text, size=SEG_LEN):
    words = text.split()
    for i in range(0, len(words), size):
        yield " ".join(words[i:i + size])


def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    files_seen = 0
    segs_inserted = 0

    for root, dirs, files in os.walk(ROOT_DIR):
        for fname in files:
            if not fname.lower().endswith(".txt"):
                continue

            path = os.path.join(root, fname)

            try:
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    text = f.read()
            except Exception as e:
                print(f"Failed to read {path}: {e}")
                continue

            if not text.strip():
                print(f"Skipping empty file: {path}")
                continue

            files_seen += 1
            segments = list(segment_text(text))
            total_segments = len(segments)

            if total_segments == 0:
                print(f"No segments produced for {path}")
                continue

            for idx, seg in enumerate(segments, start=1):
                title = f"{fname} [seg {idx:04d}]"
                extra = json.dumps({
                    "path": path,
                    "segment_index": idx,
                    "total_segments": total_segments
                })

                cur.execute(
                    """
                    INSERT INTO documents (domain, source_type, title, text, extra)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (DOMAIN, SOURCE_TYPE, title, seg, extra)
                )
                segs_inserted += 1

            conn.commit()
            print(f"Ingested {total_segments} segments from {path}")

    conn.close()
    print(f"Done. Files seen: {files_seen}, segments inserted: {segs_inserted}")


if __name__ == "__main__":
    main()