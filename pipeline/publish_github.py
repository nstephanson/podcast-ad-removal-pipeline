#!/usr/bin/env python3
"""Publish the newest cleaned episode to GitHub (releases + feed).

1. Takes the newest cleaned (non-skipped) episode from the podcast's state.
2. Creates a GitHub release (tag = episode slug) and uploads the 128k
   cleaned master as a release asset (GitHub allows 2GB/asset).
3. Records the asset's browser_download_url in state as enclosure_url.
4. Regenerates the feed via run_pipeline.write_feed.
5. Pushes the feed to the repo via the Contents API.

Usage: publish_github.py --podcast <id>
Prints the feed URL on success. Exit non-zero on failure.
Uses ~/workspace/skills/github/bin/gh.py for authenticated API calls.
"""
import argparse
import base64, json, os, re, subprocess, sys

BASE_DIR = os.path.expanduser("~/workspace/podcast-ads")
GH = os.path.expanduser("~/workspace/skills/github/bin/gh.py")

sys.path.insert(0, os.path.join(BASE_DIR, "pipeline"))
from run_pipeline import (
    write_feed, fetch_feed, load_state, save_state, init_podcast,
)


def gh(*args):
    r = subprocess.run([GH, *args], capture_output=True, text=True)
    if r.returncode != 0:
        print(f"gh.py failed: {r.stderr.strip()}", file=sys.stderr)
        sys.exit(2)
    return json.loads(r.stdout)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--podcast", required=True)
    args = ap.parse_args()
    cfg, pcfg = init_podcast(args.podcast)

    OWNER, REPO = cfg["github_owner"], cfg["github_repo"]
    FEED_PATH = pcfg["github_feed_path"]
    FEED_URL = (f"https://raw.githubusercontent.com/{OWNER}/{REPO}/main/{FEED_PATH}")

    # run_pipeline globals are set by init_podcast; import the module for them
    import run_pipeline as rp

    state = load_state()
    guid, ep = None, None
    for g, e in state["processed"].items():
        # publish oldest unpublished first
        if not e.get("skipped") and "file" in e and not e.get("enclosure_url"):
            guid, ep = g, e
            break
    if not ep:
        print("no cleaned episode in state", file=sys.stderr)
        sys.exit(1)

    slug = re.sub(r"[^a-z0-9]+", "-", ep["title"].lower()).strip("-")[:60]
    master = os.path.join(rp.SITE_DIR, ep["file"])
    if not os.path.exists(master):
        print(f"no audio file for {ep['title']}", file=sys.stderr)
        sys.exit(1)

    tag = slug[:40].strip("-") or "episode"
    # 1. create release
    with open("/tmp/gh_rel.json", "w") as f:
        json.dump({"tag_name": tag, "name": f"{pcfg['name']}: {ep['title']}",
                   "body": f"Cleaned episode — {ep.get('ads_removed',0)} ad blocks removed."}, f)
    rel = gh("POST", f"/repos/{OWNER}/{REPO}/releases", "/tmp/gh_rel.json")
    # 2. upload main cleaned asset
    asset = gh("UPLOAD", rel["upload_url"], master, "audio/mpeg")
    durl = asset["browser_download_url"]
    ep["enclosure_url"] = durl
    ep["enclosure_bytes"] = asset["size"]
    ep.pop("enclosure_expires_at", None)
    # 2b. upload ads-reel QA file if present
    ads_reel_url = None
    if ep.get("ads_reel"):
        reel_path = os.path.join(rp.SITE_DIR, ep["ads_reel"])
        if os.path.exists(reel_path):
            reel_asset = gh("UPLOAD", rel["upload_url"], reel_path, "audio/mpeg")
            ads_reel_url = reel_asset["browser_download_url"]
            ep["ads_reel_url"] = ads_reel_url
            ep["ads_reel_bytes"] = reel_asset["size"]
            log_msg = f"ads-reel uploaded: {ep['ads_reel']}"
            print(log_msg)
    # 2c. upload suspect reel if present (2026-10-22, per Nathan).
    # Not a feed item — linked in the nightly message for his review.
    if ep.get("suspects_reel"):
        sus_path = os.path.join(rp.SITE_DIR, ep["suspects_reel"])
        if os.path.exists(sus_path):
            sus_asset = gh("UPLOAD", rel["upload_url"], sus_path, "audio/mpeg")
            ep["suspects_url"] = sus_asset["browser_download_url"]
            ep["suspects_bytes"] = sus_asset["size"]
            print(f"suspects-reel uploaded: {ep['suspects_reel']}")
    save_state(state)
    # 3. regenerate feed
    xml = fetch_feed()
    feed = write_feed(state, rp.SITE_DIR, xml)
    # 4. push feed (need current SHA; create if missing)
    try:
        cur = gh("GET", f"/repos/{OWNER}/{REPO}/contents/{FEED_PATH}")
        sha = cur["sha"]
    except SystemExit:
        sha = None
    with open(os.path.join(rp.SITE_DIR, "feed.xml"), "rb") as f:
        content = base64.b64encode(f.read()).decode()
    payload = {"message": f"Update feed: {ep['title']}", "content": content}
    if sha:
        payload["sha"] = sha
    with open("/tmp/gh_feed.json", "w") as f:
        json.dump(payload, f)
    gh("PUT", f"/repos/{OWNER}/{REPO}/contents/{FEED_PATH}", "/tmp/gh_feed.json")
    print(f"feed_url={FEED_URL}")
    print(f"enclosure={durl}")


if __name__ == "__main__":
    main()
