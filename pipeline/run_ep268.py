#!/usr/bin/env python3
"""Targeted single-episode run: Unsubscribe Podcast ep 268.

Does NOT run the normal newest-first sweep. Processes only the episode
with the given guid, then regenerates the feed site file.
"""
import os, sys

TARGET_GUID = "4c3dd71c-4720-11f1-8ab7-abf8d375fb9c"

sys.path.insert(0, os.path.expanduser("~/workspace/podcast-ads/pipeline"))
import run_pipeline

cfg, pcfg = run_pipeline.init_podcast("unsubscribe")
run_pipeline.log(f"podcast: {pcfg['name']} (unsubscribe) — TARGETED run for guid {TARGET_GUID}")
state = run_pipeline.load_state()

if TARGET_GUID in state["processed"]:
    run_pipeline.log(f"guid {TARGET_GUID} already processed; nothing to do")
    sys.exit(0)

if not os.path.exists(run_pipeline.WHOOSH_PATH):
    run_pipeline.log(f"FATAL: whoosh file missing at {run_pipeline.WHOOSH_PATH}")
    sys.exit(1)

xml = run_pipeline.fetch_feed()
items = run_pipeline.parse_items(xml)
run_pipeline.log(f"feed has {len(items)} items")
item = next((it for it in items if it.get("guid") == TARGET_GUID), None)
if item is None:
    run_pipeline.log(f"FATAL: target guid {TARGET_GUID} not found in feed")
    sys.exit(1)
run_pipeline.log(f"processing: {item['title']}")
try:
    report = run_pipeline.process_episode(item, state)
    run_pipeline.log(f"report: {report['removed_seconds']:.0f}s removed in {len(report['blocks'])} blocks")
    run_pipeline.log(f"missed_sponsors: {[m['label'] for m in report['missed_sponsors']]}")
except Exception as exc:
    run_pipeline.log(f"FATAL on {item['title']}: {exc!r}")
    raise
run_pipeline.write_feed(state, run_pipeline.SITE_DIR, xml)
run_pipeline.log("feed regenerated — targeted run complete")
