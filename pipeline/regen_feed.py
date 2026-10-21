#!/usr/bin/env python3
"""Regenerate the podcast feed-site file from current live XML + state.
Used for feed-only fixes (no episode processing)."""
import os, sys
sys.path.insert(0, os.path.expanduser("~/workspace/podcast-ads/pipeline"))
import run_pipeline

podcast_id = sys.argv[1] if len(sys.argv) > 1 else "unsubscribe"
run_pipeline.init_podcast(podcast_id)
state = run_pipeline.load_state()
xml = run_pipeline.fetch_feed()
run_pipeline.write_feed(state, run_pipeline.SITE_DIR, xml)
out = os.path.join(run_pipeline.SITE_DIR, "feed.xml")

# Validate: must parse as XML
import xml.etree.ElementTree as ET
ET.parse(out)
print(f"feed regenerated and VALID: {out}")
