import sqlite3
import json

MASTER_DB = "/Volumes/SS 1TB/LMID_ex/Analysis/Everything SQL db/lmid_master.db"
BYU_DB = "/Volumes/SS 1TB/LMID_ex/Analysis/Python Scripts/3. Active Members/BYU Scrape/BYU.db"

DOMAIN = "active_members"
SOURCE_TYPE = "byu_talks_segment_420w"
SEG_LEN = 420


def segment_text(text, seg_len=SEG_LEN):
    """Split transcript text into fixed-length word segments."""
    words = text.split()
    for i in range(0, len(words), seg_len):
        chunk = words[i:i + seg_len]
        if chunk:
            yield " ".join(chunk)


def main():
    master = sqlite3.connect(MASTER_DB)
    mcur = master.cursor()

    byu = sqlite3.connect(BYU_DB)
    bcur = byu.cursor()

    bcur.execute("""
        SELECT id, title, speaker, event_type, topics,
               url, date, transcript
        FROM byu_talks
    """)
    rows = bcur.fetchall()

    talks_seen = 0
    segs_inserted = 0

    for (
        talk_id, title, speaker, event_type, topics,
        url, date, transcript
    ) in rows:

        if not transcript or not transcript.strip():
            continue

        talks_seen += 1

        segments = list(segment_text(transcript))
        total_segments = len(segments)

        for idx, seg in enumerate(segments, start=1):
            seg_title = f"{speaker} – {title} [seg {idx:04d}]"

            extra = {
                "byu_id": talk_id,
                "speaker": speaker,
                "event_type": event_type,
                "topics": topics,
                "url": url,
                "date": date,
                "segment_index": idx,
                "total_segments": total_segments,
            }

            mcur.execute(
                """
                INSERT INTO documents
                (domain, source_type, title, text, extra)
                VALUES (?, ?, ?, ?, ?)
                """,
                (DOMAIN, SOURCE_TYPE, seg_title, seg, json.dumps(extra))
            )

            segs_inserted += 1

        master.commit()
        print(f"Ingested {total_segments} segments from ID {talk_id}: {title}")

    master.close()
    byu.close()

    print("\nDone.")
    print(f"Talks processed: {talks_seen}")
    print(f"Segments inserted: {segs_inserted}")


if __name__ == "__main__":
    main()