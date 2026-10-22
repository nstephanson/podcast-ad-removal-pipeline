# Podcast Ad-Removal Pipeline

Server-side pipeline that downloads podcast episodes, transcribes them,
detects sponsor ad reads with a multi-method heat map, cuts them out
(inserting Nathan's own swish sound at every splice), verifies the splices,
and publishes cleaned episodes plus a QA reel to GitHub releases with a
regenerated RSS feed for Podcast Addict.

This repo mirrors the live pipeline. A daily sync job pushes code and
config changes from the working copy. If you change the pipeline, sync
this repo in the same step (see "Keeping this repo in sync").

## What it produces

- **Cleaned episode MP3** (128k master) as a GitHub release asset.
- **64k small copy** for chat delivery (the 128k master exceeds the ~100MB
  direct-attachment cap).
- **"Ads cut from \<episode\>" bonus feed item** — the QA reel: cut segments
  pasted in chronological order, one chapter per clip, clips listed with
  timestamps in the description.
- **Suspect reel** (2026-10-22) — `<slug>-suspects.mp3`, a release asset (NOT
  a feed item): segments the pipeline was unsure about — low-confidence
  candidates (voice anomalies, cluster-popular hits, brand-orphan windows)
  that did not become cut blocks — pasted chronologically, one chapter per
  segment. Linked in the nightly message so Nathan reviews minutes of
  uncertain audio instead of the whole episode, and says which suspects
  are ads. Never cut from the suspect reel without his say-so.
- **Unedited audit item** (2026-10-22) — when a state entry carries
  `unedited_url`, `write_feed` inserts a "[UNEDITED AUDIT] \<episode\>" bonus
  item (guid suffixed `-unedited`) pointing at the original audio,
  alongside the untouched cleaned item. Used when Nathan audits a published
  episode for missed ads. Clearing `unedited_url` from state drops the item
  on the next feed regeneration.
- **Edits report** in the episode description (plain text, at the TOP below
  a dashes dividing line, each removed segment with duration).
- **Regenerated RSS feed** (`feeds/<Podcast-Name>` in the Clean-Podcasts
  repo) for Podcast Addict.

## Repository layout

- `pipeline/run_pipeline.py` — the engine: fetch, transcribe, detect, cut,
  verify splices, remap chapters, generate feeds.
- `pipeline/publish_github.py` — publish the newest cleaned episode:
  GitHub release + feed push to the Clean-Podcasts repo.
- `pipeline/regen_feed.py` — regenerate a feed without processing episodes.
- `pipeline/publish_feed.py` — LEGACY publisher (remote-storage upload);
  superseded by `publish_github.py`. Kept for reference.
- `pipeline/run_ep268.py` — example targeted single-episode run (by guid).
- `podcasts.yaml` — podcast config: feed URLs, GitHub feed paths, house
  plugs. Add a podcast here; no code changes needed.
- `whoosh.wav` — Nathan's own swish, inserted at every splice. Canonical
  file; do not substitute a synth.
- `requirements.txt` — Python dependencies.

## Setup

- Python 3.11+, ffmpeg on PATH.
- `pip install -r requirements.txt`. The faster-whisper `base` model
  (CPU, int8) downloads automatically on first run.
- GitHub publishing needs a personal access token. This environment uses
  a `gh.py` API wrapper (see the header of `publish_github.py`) with the
  token held in secure connector storage. **Never commit a token to this
  repo.**
- Local layout expected by the scripts (BASE_DIR in `run_pipeline.py`):
  `~/workspace/podcast-ads/` containing `whoosh.wav`, `podcasts.yaml`,
  `pipeline/`, and per-podcast `podcasts/<id>/state.json` plus
  `podcasts/<id>/episodes/<slug>/` working dirs (original MP3, chunk
  transcripts, cleaned files).

## Running

```bash
python3 pipeline/run_pipeline.py --podcast <id>     # every unprocessed episode, oldest first
python3 pipeline/run_pipeline.py --podcast <id> --limit 1   # newest unprocessed only
python3 pipeline/publish_github.py --podcast <id>  # publish newest cleaned episode
```

The nightly cron runs at 8:00pm CDT across all configured podcasts
(Security Now, Unsubscribe Podcast, Lore), then publishes. A full
episode takes roughly an hour (transcription dominates).

## Detection architecture (the heat map)

Every method runs independently on the **unedited** episode, then results
are cross-checked via a per-second vote heat map. **2+ votes =
high-confidence auto-cut.** Cut points land on clean sentence breaks and
are re-verified by re-transcription. Ad-block bounds: 45s minimum, 720s
maximum.

| Method | What it detects | Confidence |
|---|---|---|
| `cta` | Generic URL / promo-code CTA patterns (10s floor for short spots) | High — cuts alone |
| `brand` | Description sponsors as ground truth (fuzzy matching, incl. Whisper mistranscription variants) | High — cuts alone |
| `cluster` | Dense brand mentions: 4+ in 1 minute (casual ad reads) | High — cuts alone |
| `duplicate` | Verbatim-repeated segments ≥15s (8-word shingles); inserted ads run 2+ times | High — cuts alone |
| `chapter` | Publisher-labeled ad chapters (`[Ad]`, `Ad:`, `Advertisement`, `Sponsor`, `Commercial`, `Ad break`) from ID3 tags and RSS `<psc:chapter>` | High — cuts alone |
| `brand-orphan` | Sponsor mentions the brand method found but could not expand into a segment (no opening cue) | Low — heat-map votes only, never cuts alone |
| `cluster-popular` | Dense mentions of popular sponsors NOT in the description | Low — heat-map votes only; single-vote hits go to the suspect reel, never auto-cut |
| `voice` | Host-voice anomaly (MFCC profile distance) | Review only — suspect reel, never cuts alone |

Notes:

- `detect_narrative_breaks` (sentence-embedding narrative jumps) is defined
  in `run_pipeline.py` but **not wired into detection**. Do not assume it
  runs.
- **Domain-stem matching (2026-10-22):** sponsor variants include the bare
  domain stem, so dotted domains with modern TLDs (`canary.tools`) match
  the spoken brand word. SN 1101's Thinkst Canary read was missed because
  the variant set contained only `canary.tools` / `canary tools`, which
  never match transcript tokens (Whisper splits spoken URLs into separate
  words, and may mangle the TLD). The TLD-strip lists in
  `extract_sponsors` and `find_url_mentions` must stay in sync — a missing
  TLD is a silent miss.
- **Dynamic ads:** two downloads of one episode differ; transcript
  differences over 20s reveal the ad slots, which are consistent across
  downloads. Slot positions map via offset subtraction (costs 2x
  transcription).
- **Ollama / LLM reviewer: evaluated 2026-10-14, REJECTED.** It hallucinated
  sponsor reads, missed real ads, produced garbled timestamps, and was
  too slow and memory-hungry. Do not re-add without a fresh evaluation.

## Standing rules (do not break these)

- The episode description's sponsor list is a **double-check for missed
  sponsors only** — never the primary ad source. Missed sponsors are
  reported (`missed_sponsors` in the report/state), never silently
  skipped.
- **Club TWiT plugs are cut like sponsor ads** on all podcasts
  (HOUSE_PLUGS in `run_pipeline.py`; `house_plugs` in `podcasts.yaml`).
- **Nathan's swish (`whoosh.wav`) at every splice.**
- **QA reel:** "Ads cut from \<episode\>", Bonus episode type, one chapter
  per clip, clips listed with timestamps in the description.
- **Edits report:** plain text at the TOP of the episode description below
  the dashes dividing line (`———————————————————`), each removed segment
  with duration. **No HTML tags** — they break Podcast Addict and other
  apps' description display. Lore's original Libsyn HTML descriptions stay
  untouched; only the prepended edits report is plain text.
- **Crash protocol:** fix the root cause, then re-run the affected episode
  from the top — full fresh run (re-download, full detection, full cut),
  verified end to end. Never patch around a crash and resume.
- **Transcription word entries have LEADING SPACES** (`" dose"`). Always
  strip whitespace (use `norm_words()`) when searching transcripts.
- **Feed links to Nathan** go as plain text in code blocks, `https://`
  prefix omitted — his Android long-press on links offers Reply/emoji
  only, no copy option. (Chat convention, not pipeline behavior.)

## What is NOT in this repo (stays local)

- Episode audio, transcripts, and `state.json` (7GB+ operational data).
  These are local bookkeeping only. The published feeds and GitHub
  releases are the source of truth for what has been processed.
- Whisper model cache (auto-downloaded by faster-whisper).
- GitHub tokens or any credentials — never committed.

## Keeping this repo in sync

The live working copy is `~/workspace/podcast-ads/` and this repo's
source of truth for code/config is mirrored from it by
`sync_pipeline_repo.py` (kept alongside the pipeline, not in this repo),
run daily at 9:00pm CDT. It refreshes `pipeline/*`, `podcasts.yaml`, and
`whoosh.wav` from the live files, then uploads only files whose blob SHA
changed. The README, `requirements.txt`, and `.gitignore` live in the
mirror directory and are synced as-is. When you change the pipeline,
update the README in the same step and run the sync script immediately —
do not wait for the nightly job.
