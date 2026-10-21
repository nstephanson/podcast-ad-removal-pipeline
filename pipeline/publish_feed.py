#!/usr/bin/env python3
"""Publish the newest cleaned episode to the RSS feed.

1. Takes the newest cleaned (non-skipped) episode from state.json.
2. Ensures a feed MP3 under ~95MB (re-encodes the 64k small copy at a lower
   bitrate only if needed; remote-storage rejects uploads >= ~100MB).
3. Uploads it via `remote-storage upload-file --ttl-hours 48`.
4. Records enclosure_url / enclosure_bytes / enclosure_expires_at in state.json.
5. Regenerates ~/workspace/podcast-ads/feed-site/feed.xml from the live
   upstream feed via run_pipeline.write_feed.

Prints the enclosure URL and expiry on success. Exit non-zero on failure.
"""
import json, os, re, subprocess, sys

BASE_DIR = os.path.expanduser("~/workspace/podcast-ads")
SITE_DIR = os.path.join(BASE_DIR, "feed-site")
STATE_PATH = os.path.join(BASE_DIR, "state.json")
MAX_FEED_BYTES = 95 * 1024 * 1024  # stay under remote-storage's ~100MB cap

sys.path.insert(0, os.path.join(BASE_DIR, "pipeline"))
from run_pipeline import write_feed, fetch_feed, load_state, save_state


def main():
    state = load_state()
    # newest cleaned episode = last inserted non-skipped with audio
    guid, ep = None, None
    for g, e in state["processed"].items():
        if not e.get("skipped") and ("small_file" in e or "file" in e):
            guid, ep = g, e
    if not ep:
        print("no cleaned episode in state", file=sys.stderr)
        sys.exit(1)

    small_rel = ep.get("small_file")
    small_path = os.path.join(BASE_DIR, small_rel) if small_rel else None
    if not small_path or not os.path.exists(small_path):
        print(f"small copy missing: {small_rel}", file=sys.stderr)
        sys.exit(1)

    feed_path = small_path
    if os.path.getsize(small_path) > MAX_FEED_BYTES:
        # re-encode at a bitrate guaranteed to fit
        dur = ep.get("cleaned_seconds") or 3 * 3600
        bitrate = max(32000, int(MAX_FEED_BYTES * 0.95 * 8 / dur / 1000) * 1000)
        feed_path = os.path.join(SITE_DIR, "feed-audio.mp3")
        subprocess.run(["ffmpeg", "-y", "-v", "error", "-i", small_path,
                        "-c:a", "libmp3lame", "-b:a", f"{bitrate // 1000}k",
                        feed_path], check=True)
        if os.path.getsize(feed_path) > MAX_FEED_BYTES:
            print("re-encoded feed file still too large", file=sys.stderr)
            sys.exit(1)

    out = subprocess.run(
        ["/opt/hatch/bin/remote-storage", "upload-file",
         "--path", feed_path, "--ttl-hours", "48"],
        capture_output=True, text=True)
    if out.returncode != 0:
        print(f"upload failed: {out.stderr.strip()}", file=sys.stderr)
        sys.exit(1)
    info = json.loads(out.stdout)
    ep["enclosure_url"] = info["url"]
    ep["enclosure_bytes"] = info["bytes"]
    ep["enclosure_expires_at"] = info["expires_at"]
    save_state(state)

    xml = fetch_feed()
    write_feed(state, SITE_DIR, xml)
    print(f"enclosure_url={info['url']}")
    print(f"expires_at={info['expires_at']}")


if __name__ == "__main__":
    main()
