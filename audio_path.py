import subprocess
import os

# point directly to the yt-dlp inside this script’s venv
YT_DLP_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "venv", "bin", "yt-dlp")
)

def download_audio(video_id, out_dir="audio"):
    """
    Basic wrapper around yt-dlp to pull audio for a given YouTube ID.
    Returns the local path if successful, otherwise None.
    """

    out_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), out_dir))
    os.makedirs(out_dir, exist_ok=True)

    url = f"https://www.youtube.com/watch?v={video_id}"
    out_file = os.path.join(out_dir, f"{video_id}.m4a")

    try:
        subprocess.run(
            [
                YT_DLP_PATH,
                "-f", "bestaudio[ext=m4a]/bestaudio",
                "-o", out_file,
                url
            ],
            check=True
        )
        return out_file

    except Exception as e:
        print("yt-dlp error:", e)
        return None