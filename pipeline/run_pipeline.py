#!/usr/bin/env python3
"""
Podcast ad-removal pipeline (podcast-agnostic).

Each run (per podcast, via --podcast <id>):
  1. Fetches the podcast's RSS feed
  2. Finds episodes not yet processed (per-podcast state tracks GUIDs)
  3. Downloads the episode MP3
  4. Transcribes with faster-whisper (chunked, resumable)
  5. Extracts sponsor brands from the RSS description (ground truth)
  6. Locates each sponsor's ad-read block in the transcript
  7. Cuts the ad blocks, inserting a whoosh transition at each splice
  8. Verifies splices by re-transcribing clips around each cut
  9. Remaps pre-existing chapter timestamps for the cuts, or
     auto-generates chapters from topic changes if none exist
 10. Writes a per-episode report and regenerates the clean RSS feed

Podcasts are configured in ~/workspace/podcast-ads/podcasts.yaml —
no code changes needed to add one.

Safety: conservative cuts only. A block is cut only when a sponsor brand is
mentioned AND an opening cue phrase precedes it AND a closing boundary
(silence gap or return cue) is found within sane duration bounds. If detection
is uncertain for an episode, the episode is left whole and flagged.
"""

import json
import os
import re
from datetime import datetime, timezone
import subprocess
import sys
import time
import urllib.request
from html import unescape

# The model snapshot lives in the default HF cache; force offline so the
# (broken-proxy) network is never touched.
os.environ["HF_HUB_OFFLINE"] = "1"

BASE_DIR = os.path.expanduser("~/workspace/podcast-ads")
WHOOSH_PATH = os.path.join(BASE_DIR, "whoosh.wav")
MODEL_DIR = os.path.join(BASE_DIR, "model")
CONFIG_PATH = os.path.join(BASE_DIR, "podcasts.yaml")

# Ad-block bounds (seconds)
MIN_AD_LEN = 45
MAX_AD_LEN = 12 * 60
# Chunk size for transcription (seconds) — keeps each job short
TRANSCRIBE_CHUNK = 180

# Popular podcast sponsors not in the episode description.
# Used ONLY by the cluster finder (4+ mentions in 1 min) as a safety net
# for unlisted/dynamic ads — never by the brand matcher. Hits here are
# low-confidence: auto-cut only with 2+ method votes (heat map).
# Variants include common Whisper mistranscriptions (e.g. mitt->mint).
POPULAR_BRANDS = [
    {"label": "Mint Mobile", "variants": ["mint mobile", "mint", "mitt mobile", "mitt"]},
    {"label": "BetterHelp", "variants": ["betterhelp", "better help"]},
    {"label": "Shopify", "variants": ["shopify"]},
    {"label": "Squarespace", "variants": ["squarespace", "square space"]},
    {"label": "Indeed", "variants": ["indeed"]},
    {"label": "LinkedIn", "variants": ["linkedin", "linked in"]},
    {"label": "Aura", "variants": ["aura"]},
    {"label": "Rocket Money", "variants": ["rocket money"]},
    {"label": "HelloFresh", "variants": ["hellofresh", "hello fresh"]},
    {"label": "Factor", "variants": ["factor", "factor meals"]},
    {"label": "AG1", "variants": ["ag1", "ag 1", "athletic greens"]},
    {"label": "Manscaped", "variants": ["manscaped", "man scaped"]},
    {"label": "Seed", "variants": ["seed", "seed probiotic"]},
    {"label": "Zocdoc", "variants": ["zocdoc", "zoc doc"]},
    {"label": "Progressive", "variants": ["progressive"]},
    {"label": "SimpliSafe", "variants": ["simplisafe", "simpli safe"]},
    {"label": "Quince", "variants": ["quince"]},
]

# Chapter-title ad markers (method 'chapter', added 2026-10-14 per Nathan —
# idea borrowed from the podcast-ad-cleaner project). Some podcasts label ad
# segments directly in their chapter titles ("[Ad]", "Ad:", "Sponsor", ...).
# A publisher-labeled ad chapter is high-precision, so it votes like the
# other high-precision methods and is cut even on a single vote.
# Deliberately does NOT match "advertising" (industry discussion).
CHAPTER_AD_PATTERNS = [
    r'\[\s*ads?\s*\]',          # [Ad], [Ads]
    r'^ads?\s*[:\-–—]',         # "Ad: ...", "Ads - ..."
    r'\badvertisements?\b',     # Advertisement(s)
    r'\bsponsors?(hip|ed)?\b',  # Sponsor, Sponsors, Sponsored, Sponsorship
    r'\bcommercials?(\s+break)?\b',  # Commercial, Commercial break
    r'\bad\s+break\b',          # Ad break
    r'\badbreak\b',
]


def load_config(podcast_id):
    """Load podcasts.yaml and return (global_cfg, podcast_cfg)."""
    import yaml
    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
    pcfg = cfg["podcasts"].get(podcast_id)
    if not pcfg:
        raise SystemExit(f"unknown podcast id '{podcast_id}' in {CONFIG_PATH}")
    return cfg, pcfg


def podcast_paths(podcast_id):
    """Per-podcast working paths."""
    root = os.path.join(BASE_DIR, "podcasts", podcast_id)
    return {
        "root": root,
        "ep_dir": os.path.join(root, "episodes"),
        "site_dir": os.path.join(root, "feed-site"),
        "state_path": os.path.join(root, "state.json"),
    }


def house_plugs(pcfg):
    """House plugs configured for this podcast, as {label, variants} set."""
    return [{"label": p["label"], "variants": set(p["variants"])}
            for p in pcfg.get("house_plugs", [])]


def github_feed_url(cfg, pcfg):
    return (f"https://raw.githubusercontent.com/{cfg['github_owner']}/"
            f"{cfg['github_repo']}/main/{pcfg['github_feed_path']}")


# Per-podcast globals, populated by main() from podcasts.yaml.
# (Kept as globals so the pipeline functions stay unchanged.)
FEED_URL = None
EP_DIR = None
SITE_DIR = None
STATE_PATH = None
HOUSE_PLUGS = []
GITHUB_FEED_URL = None


def init_podcast(podcast_id):
    """Load config and set per-podcast globals. Returns (cfg, pcfg)."""
    global FEED_URL, EP_DIR, SITE_DIR, STATE_PATH, HOUSE_PLUGS, GITHUB_FEED_URL
    cfg, pcfg = load_config(podcast_id)
    paths = podcast_paths(podcast_id)
    FEED_URL = pcfg["feed_url"]
    EP_DIR = paths["ep_dir"]
    SITE_DIR = paths["site_dir"]
    STATE_PATH = paths["state_path"]
    HOUSE_PLUGS = house_plugs(pcfg)
    GITHUB_FEED_URL = github_feed_url(cfg, pcfg)
    for d in (paths["root"], EP_DIR, SITE_DIR):
        os.makedirs(d, exist_ok=True)
    return cfg, pcfg

OPEN_CUES = [
    "brought to you by",
    "brought to you",
    "sponsor",
    "advertiser",
    "a word from",
    "this episode is brought",
    "this portion of",
    "today's sponsor",
]
RETURN_CUES = [
    "welcome back",
    "we're back",
    "we are back",
    "all right",
    "alright",
]


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def fetch_feed():
    req = urllib.request.Request(FEED_URL, headers={"User-Agent": "podcast-ad-pipeline/1.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read().decode("utf-8", errors="replace")


def _parse_chapter_start(s):
    """Parse a chapter start like '10:18' or '1:02:03' to seconds."""
    parts = s.strip().split(':')
    try:
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
        if len(parts) == 2:
            return int(parts[0]) * 60 + float(parts[1])
        return float(parts[0])
    except ValueError:
        return None


def _parse_rss_chapters(body):
    """Inline RSS chapter markers (<psc:chapter start title>).
    Returns [(start_sec, title)]."""
    out = []
    for cm in re.finditer(r'<psc:chapter\b[^>]*>', body):
        tag = cm.group(0)
        sm = re.search(r'start="([^"]+)"', tag)
        tm = re.search(r'title="([^"]+)"', tag)
        if not sm or not tm:
            continue
        sec = _parse_chapter_start(sm.group(1))
        if sec is None:
            continue
        out.append((sec, unescape(tm.group(1)).strip()))
    return out


def parse_items(xml):
    items = []
    for m in re.finditer(r"<item>(.*?)</item>", xml, re.S):
        body = m.group(1)
        def tag(name):
            mm = re.search(rf"<{name}[^>]*>(.*?)</{name}>", body, re.S)
            if not mm:
                return ""
            val = unescape(mm.group(1)).strip()
            # Strip CDATA wrapper (html.unescape doesn't handle it)
            if val.startswith("<![CDATA[") and val.endswith("]]>"):
                val = val[9:-3].strip()
            return val
        enc = re.search(r'<enclosure[^>]*url="([^"]+)"', body)
        items.append({
            "guid": tag("guid") or (enc.group(1) if enc else ""),
            "title": tag("title"),
            "pubDate": tag("pubDate"),
            "description": tag("description"),
            "audio_url": enc.group(1) if enc else "",
            "rss_chapters": _parse_rss_chapters(body),
        })
    return items


def extract_sponsors(description_html):
    """Sponsor brand names from the RSS description.

    Two sources: rel="sponsored" links (ground truth), and plain-text
    sponsor lists ("THANK YOU TO OUR SPONSORS! AG1 ... SHOPIFY ...").
    """
    brands = []
    for m in re.finditer(r'<a[^>]*rel="sponsored"[^>]*>(.*?)</a>', description_html, re.I | re.S):
        text = re.sub(r"<[^>]+>", "", m.group(1)).strip().lower()
        text = re.sub(r"^https?://(www\.)?", "", text)
        text = text.split("/")[0]
        root = re.sub(r"\.(com|net|org|io|ai|tv|co|us)$", "", text)
        variants = {root, root.replace(".", " "), root.replace("-", " ")}
        # strip common marketing prefixes so the spoken brand is matched,
        # not just the URL token (2026-10-21: "joindeleteme" only matched
        # the URL near the end of the SN 1101 DeleteMe read, so no opening
        # cue was found and the whole read was missed)
        for prefix in ("drink", "get", "try", "take", "shop", "join"):
            if root.startswith(prefix) and len(root) - len(prefix) >= 4:
                variants.add(root[len(prefix):])
        # drop bare single common words that would false-positive
        variants = {v for v in variants if not (len(v.split()) == 1 and v in
                    {"material", "security", "one", "shield", "guard"})}
        if not variants:
            variants = {root.replace(".", " ")}
        brands.append({"label": text, "variants": {v for v in variants if v}})
    # Plain-text sponsor lists: "THANK YOU TO OUR SPONSORS! AG1 ... SHOPIFY ..."
    # Brand names come from the linked domains (ground truth); ALL-CAPS
    # names in the text supply extra match variants.
    text = re.sub(r"<[^>]+>", " ", description_html)
    text = unescape(text)
    sm = re.search(
        r"(thank you to our sponsors|thanks to our sponsors|series sponsor|our sponsors|sponsored by|sponsors?\s*:)"
        r"[:!\s]*(.{0,2000})", text, re.I | re.S)
    if sm:
        section = sm.group(2)
        # cut at the next all-caps header, social-links block, or long
        # dash/em-dash separator run (Lore/Libsyn "Sponsors:" format)
        section = re.split(r"\b(FOLLOW OUR|PATREON|MERCH|SUBSCRIBE)\b|[-—–]{3,}",
                           section, 1, flags=re.I)[0]
        # ALL-CAPS display names for variant enrichment
        display = []
        for cm in re.finditer(r"\b([A-Z][A-Z0-9&']{1,20}(?: [A-Z][A-Z0-9&']{1,20}){0,2})\b",
                             section):
            name = cm.group(1).strip()
            if name.upper() in {"THANK YOU TO OUR SPONSORS", "THANK YOU", "OUR SPONSORS",
                                "SPONSORS", "AD", "SERIES SPONSOR"} or len(name) < 2:
                continue
            display.append(name)
        # Title-Case display names (Libsyn "Series Sponsor: Warby Parker" format)
        for cm in re.finditer(r"\b([A-Z][a-z]{2,19}(?: [A-Z][a-z]{2,19}){0,2})\b",
                             section):
            name = cm.group(1).strip()
            if name.upper() in {"SERIES SPONSOR", "SPONSOR", "SPONSORS"} or len(name) < 4:
                continue
            display.append(name)
        # domains -> brand roots (scheme optional: Libsyn prints "WarbyParker.com/LORE")
        for um in re.finditer(r"(?<![\w:/.-])(?:https?://)?(?:www\.)?([a-z0-9][a-z0-9.-]*\.[a-z]{2,})(?![a-z])", section, re.I):
            domain = um.group(1).lower()
            # skip social/video platforms
            if any(p in domain for p in ("instagram", "tiktok", "twitter", "x.com",
                                         "facebook", "youtube", "patreon", "pepperbox")):
                continue
            host = domain.split("/")[0]
            root = re.sub(r"\.(com|net|org|io|ai|tv|co|us)$", "", host)
            # strip common marketing prefixes (drink, get, try, take, shop, join)
            root_stripped = re.sub(r"^(drink|get|try|take|shop|join)", "", root)
            if not root or len(root) < 2:
                continue
            # find a display name containing the root, for a nicer label
            label = root
            variants = {root}
            # only use stripped variant if it's a plausible word (4+ chars)
            # e.g., "takeultra"->"ultra" yes, "shopify"->"ify" no
            if (root_stripped and root_stripped != root
                    and len(root_stripped) >= 4):
                variants.add(root_stripped)
            for d in display:
                dl = d.lower().replace(" ", "")
                # match if root in display, display in root, or share 4+ char substring
                match = (root in dl or dl in root.replace(" ", ""))
                if not match:
                    # check for shared distinctive substring
                    for i in range(len(root) - 3):
                        sub = root[i:i+4]
                        if len(sub) >= 4 and sub in dl and sub not in (
                                "http", "www."):
                            match = True
                            break
                if match:
                    label = d
                    variants.add(d.lower())
                    variants.add(dl)
                    # also add individual words from display name
                    for w in d.lower().split():
                        if len(w) >= 4:
                            variants.add(w)
                    break
            brands.append({"label": label, "variants": variants})
    # de-dupe preserving order
    seen, out = set(), []
    for b in brands:
        if b["label"] not in seen:
            seen.add(b["label"])
            out.append(b)
    return out


def load_state():
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH) as f:
            return json.load(f)
    return {"processed": {}, "base_url": ""}


def save_state(state):
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, STATE_PATH)


def download(url, dest, retries=6):
    if os.path.exists(dest) and os.path.getsize(dest) > 0:
        log(f"already downloaded: {dest}")
        return
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            if attempt > 1:
                log(f"downloading {url} (attempt {attempt}/{retries})")
            else:
                log(f"downloading {url}")
            req = urllib.request.Request(url, headers={"User-Agent": "podcast-ad-pipeline/1.0"})
            with urllib.request.urlopen(req, timeout=120) as r, open(dest, "wb") as f:
                while True:
                    chunk = r.read(1 << 20)
                    if not chunk:
                        break
                    f.write(chunk)
            log(f"saved {os.path.getsize(dest)} bytes")
            return
        except Exception as e:
            last_err = e
            # drop the partial file so a retry starts clean
            try:
                if os.path.exists(dest):
                    os.remove(dest)
            except OSError:
                pass
            log(f"download attempt {attempt} failed: {type(e).__name__}: {e}")
            if attempt < retries:
                wait = 20 * attempt
                log(f"retrying download in {wait}s...")
                time.sleep(wait)
    raise last_err


def probe_duration(path):
    out = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "csv=p=0", path],
        capture_output=True, text=True, check=True)
    return float(out.stdout.strip())


def load_model():
    from faster_whisper import WhisperModel
    return WhisperModel("base", device="cpu", compute_type="int8",
                        local_files_only=True)


def transcribe_file(model, wav_path, **kwargs):
    """Transcribe via a numpy array.

    faster-whisper 1.2.1 passes metadata_errors to av.open(), which the
    installed PyAV 19 no longer accepts — feeding raw audio avoids av.
    """
    import numpy as np
    out = subprocess.run(
        ["ffmpeg", "-v", "error", "-i", wav_path, "-ar", "16000", "-ac", "1",
         "-f", "f32le", "-acodec", "pcm_f32le", "-"],
        capture_output=True, check=True).stdout
    audio = np.frombuffer(out, dtype=np.float32)
    return model.transcribe(audio, **kwargs)


def transcribe_chunked(mp3_path, work_dir):
    """Transcribe in short chunks with word timestamps; resumable."""
    from faster_whisper import WhisperModel
    os.makedirs(work_dir, exist_ok=True)
    duration = probe_duration(mp3_path)
    model = load_model()
    words = []  # (start, end, text)
    n_chunks = int(duration // TRANSCRIBE_CHUNK) + 1
    for i in range(n_chunks):
        start = i * TRANSCRIBE_CHUNK
        if start >= duration:
            break
        chunk_json = os.path.join(work_dir, f"chunk_{i:03d}.json")
        if os.path.exists(chunk_json):
            with open(chunk_json) as f:
                data = json.load(f)
            words.extend([(w[0], w[1], w[2]) for w in data["words"]])
            continue
        chunk_wav = os.path.join(work_dir, f"chunk_{i:03d}.wav")
        subprocess.run(
            ["ffmpeg", "-y", "-v", "error", "-ss", str(start),
             "-t", str(TRANSCRIBE_CHUNK), "-i", mp3_path,
             "-ar", "16000", "-ac", "1", chunk_wav],
            check=True)
        segments, _ = transcribe_file(model, chunk_wav, word_timestamps=True)
        cwords = []
        for seg in segments:
            for w in (seg.words or []):
                cwords.append((w.start + start, w.end + start, w.word))
        with open(chunk_json, "w") as f:
            json.dump({"words": cwords}, f)
        os.remove(chunk_wav)
        words.extend(cwords)
        log(f"transcribed chunk {i + 1}/{n_chunks}")
    full = os.path.join(work_dir, "transcript.json")
    with open(full, "w") as f:
        json.dump({"words": words, "duration": duration}, f)
    return words, duration


def find_silences(mp3_path, work_dir, min_dur=1.5):
    """Return list of (silence_start, silence_end) via ffmpeg silencedetect."""
    out = subprocess.run(
        ["ffmpeg", "-v", "info", "-i", mp3_path, "-af",
         f"silencedetect=noise=-35dB:d={min_dur}", "-f", "null", "-"],
        capture_output=True, text=True).stderr
    silences = []
    starts = re.findall(r"silence_start: ([\d.]+)", out)
    ends = re.findall(r"silence_end: ([\d.]+)", out)
    for s, e in zip(starts, ends):
        silences.append((float(s), float(e)))
    return silences


def norm_words(words):
    """Whitespace-normalized lowercase words for matching (indices align)."""
    return [re.sub(r"\s+", " ", w[2]).strip().lower() for w in words]


def find_mentions(nw, variants):
    """Word indices mentioning a brand.

    Handles 'bit warden' -> 'bitwarden' (split words) and whisper
    mis-transcriptions like 'threadlocker' -> 'threatlocker' (fuzzy).
    """
    from difflib import SequenceMatcher
    hits = []
    L = len(nw)
    for v in variants:
        toks = v.split()
        if len(toks) == 1 and len(v) >= 6:
            for i in range(L):
                w = nw[i]
                if v in w or SequenceMatcher(None, v, w).ratio() >= 0.8:
                    hits.append(i)
            for i in range(L - 1):
                if v in (nw[i] + nw[i + 1]):
                    hits.append(i)
            continue
        n = len(toks)
        for i in range(L - n + 1):
            if all(toks[j] in nw[i + j] for j in range(n)):
                hits.append(i)
    return sorted(set(hits))


def find_produced_spot(words, nw, hits, silences):
    """Fallback for produced spots with no host cue (e.g. Guardsquare).

    Pattern: brand mention + '.com' URL + 'thank ... support' sign-off
    clustered together. Returns (start, end) or None.
    """
    anchor = anchor_hit = None
    for h in hits:
        for i in range(h, min(len(nw), h + 200)):
            window = " ".join(nw[i:i + 10])
            if "thank" in window and "support" in window:
                anchor, anchor_hit = i, h
                break
        if anchor is not None:
            break
    if anchor is None:
        return None
    if not any(".com" in nw[i] for i in range(anchor_hit, min(len(nw), anchor + 12))):
        return None
    t_anchor = words[anchor][0]
    cluster_start_t = t_anchor
    for h in hits:
        t = words[h][0]
        if t_anchor - 60 <= t <= t_anchor and t < cluster_start_t:
            cluster_start_t = t
    seg_start = max(0.0, cluster_start_t - 8.0)
    for s_start, s_end in silences:
        if s_end <= cluster_start_t and s_end >= cluster_start_t - 30:
            seg_start = s_end  # closest silence before the spot
    # end: thanks sign-off (plus 'Back to Steve'-style handoff), then pause
    t = next((k for k in range(anchor, min(anchor + 10, len(nw))) if "thank" in nw[k]), anchor)
    seg_end = snap_forward(get_pauses(words, silences), thanks_end(words, nw, t))
    dur = seg_end - seg_start
    if not (15 <= dur <= 150):
        return None
    return seg_start, seg_end


def get_pauses(words, silences):
    """Pauses from word gaps (>=0.7s) union silencedetect silences."""
    pauses = [(s, e) for s, e in silences]
    for a, b in zip(words, words[1:]):
        if b[0] - a[1] >= 0.7:
            pauses.append((a[1], b[0]))
    pauses.sort()
    return pauses


def thanks_end(words, nw, t):
    """End time of a thanks sign-off: the thanks sentence plus a following
    sign-off sentence ('happy to support you right back', 'Back to Steve')."""
    end_idx = t
    for j in range(t, min(t + 15, len(words))):
        if nw[j][-1:] in ".!?":
            end_idx = j
            break
    nxt = end_idx + 1
    if nxt < len(words):
        sent = " ".join(nw[nxt:nxt + 12])
        if any(k in sent for k in ("support", "happy to", "love them",
                                   "back to steve", "back to leo")):
            for j in range(nxt, min(nxt + 15, len(words))):
                if nw[j][-1:] in ".!?":
                    end_idx = j
                    break
    return words[end_idx][1]


def snap_forward(pauses, end_t, max_gap=4.0):
    for s_start, s_end in pauses:
        if s_start >= end_t and s_start - end_t <= max_gap and (s_end - s_start) >= 0.6:
            return s_start + 0.1
    return end_t + 0.2


def find_opening(words, nw, silences, first):
    """Find where a host-read ad starts. Returns seg_start or None."""
    open_idx = None
    for i in range(first, max(-1, first - 200), -1):
        window = " ".join(nw[max(0, i - 8):i + 1])
        if any(cue in window for cue in OPEN_CUES):
            open_idx = i
            break
    if open_idx is None:
        return None
    # snap the cut to the pause before the ad opening (clean sentence break):
    # locate the cue phrase's first word, then take the latest pause before it
    cue_start = open_idx
    win_start = max(0, open_idx - 8)
    for j in range(win_start, open_idx + 1):
        if any(cue in " ".join(nw[j:j + 7]) for cue in OPEN_CUES):
            cue_start = j
            break
    cue_t = words[cue_start][0]
    # Prefer the latest sentence-ending word before the cue: a clean
    # sentence break even when the pause there is short. The long-pause
    # rule below can otherwise snap back to a mid-sentence comma pause
    # (SN 1101 DeleteMe: it snapped to 2610, mid-teaser, instead of
    # "Take it away." at 2619).
    for w in reversed(words):
        if w[1] > cue_t:
            continue
        if w[1] < cue_t - 30:
            break
        if w[2].strip().endswith((".", "!", "?")):
            return w[1]
    for p_start, p_end in reversed(get_pauses(words, silences)):
        if p_end <= cue_t and p_end >= cue_t - 30 and (p_end - p_start) >= 0.6:
            return p_start
    return max(0.0, words[max(0, open_idx - 10)][0] - 0.5)


def find_closing(words, nw, silences, last, seg_start):
    """Find where a host-read ad ends. Returns end time or None."""
    # 1. thanks/support sign-off within 250 words after last brand mention;
    #    extend through the sign-off, then take the nearest pause.
    pauses = get_pauses(words, silences)
    for i in range(last, min(len(nw), last + 250)):
        window = " ".join(nw[i:i + 10])
        if "thank" in window and ("support" in window or "sponsor" in window):
            t = next((k for k in range(i, min(i + 10, len(nw))) if "thank" in nw[k]), i)
            return snap_forward(pauses, thanks_end(words, nw, t))
    # 2. first long silence after the last mention (don't run away with it)
    close_after = words[min(last + 5, len(words) - 1)][1]
    for s_start, s_end in silences:
        if s_start >= close_after and (s_end - s_start) >= 1.5:
            if s_start - seg_start <= MAX_AD_LEN and s_start - close_after <= 120:
                return s_start + 0.2
            break
    # 3. return cue within 400 words
    for i in range(last, min(len(words), last + 400)):
        window = " ".join(nw[i:i + 4])
        if any(cue in window for cue in RETURN_CUES):
            if words[i][0] - seg_start <= MAX_AD_LEN:
                return float(words[i][0])
    return None


def detect_ad_blocks(words, sponsors, silences, mp3_path=None, chapters=None):
    """Locate ad-read blocks via multi-method heat map (2026-10-09).

    Each detection method runs independently on the unedited episode,
    then results are cross-checked via a per-second vote heat map.
    Segments with 2+ method votes are high-confidence ads.
    Single-method hits are kept only if that method is high-precision.

    Methods:
    - cta: Generic URL/promo-code CTA patterns
    - brand: Description sponsor brand mentions
    - cluster: Dense brand-mention clusters (casual ad reads)
    - cluster-popular: Dense mentions of popular sponsors not in description
      (low-confidence: auto-cut only with 2+ votes)
    - duplicate: Verbatim-repeated segments (inserted ads run 2+ times)
    - voice: Host-voice anomaly (segments far from host voice profile)
    - chapter: Publisher-labeled ad chapters, e.g. '[Ad]', 'Sponsor'
      (2026-10-14; high-precision like brand/cta)
    - brand-orphan: Sponsor mentions _expand_segment could not expand
      (no opening cue). Low-confidence: heat-map votes only, never cuts
      alone (2026-10-14; Unsubscribe 268 DOSE/AG1 were silently dropped)

    Returns [(start, end, label)]."""
    # Run each method independently, collecting (start, end, label, method)
    candidates = []
    candidates.extend([(s, e, l, 'cta') for s, e, l in _method_cta(words, sponsors, silences)])
    candidates.extend([(s, e, l, 'brand') for s, e, l in _method_brand(words, sponsors, silences)])
    candidates.extend([(s, e, l, 'brand-orphan') for s, e, l in _method_brand_orphan(words, sponsors, silences)])
    candidates.extend([(s, e, l, 'cluster') for s, e, l in _method_cluster(words, sponsors, silences)])
    candidates.extend([(s, e, l, 'cluster-popular') for s, e, l in _method_cluster_popular(words, sponsors, silences)])
    candidates.extend([(s, e, l, 'duplicate') for s, e, l in find_duplicate_segments(words)])
    candidates.extend([(s, e, l, 'chapter') for s, e, l in _method_chapter(words, chapters, silences)])
    if mp3_path:
        candidates.extend([(s, e, l, 'voice') for s, e, l in _method_voice(words, mp3_path)])

    if not candidates:
        return []

    # Build heat map: per-second vote counts
    duration = int(words[-1][0]) + 1
    votes = [0] * duration
    vote_detail = [[] for _ in range(duration)]  # which methods voted
    for s, e, label, method in candidates:
        for sec in range(int(s), min(int(e) + 1, duration)):
            votes[sec] += 1
            vote_detail[sec].append((method, label))

    # High-confidence: 2+ methods agree. Merge contiguous.
    blocks = []
    in_block = False
    bs = 0
    block_methods = set()
    for sec in range(duration):
        if votes[sec] >= 2 and not in_block:
            in_block = True
            bs = sec
            block_methods = set(m for m, _ in vote_detail[sec])
        elif votes[sec] >= 2 and in_block:
            block_methods.update(m for m, _ in vote_detail[sec])
        elif votes[sec] < 2 and in_block:
            in_block = False
            be = sec
            dur = be - bs
            if MIN_AD_LEN <= dur <= MAX_AD_LEN:
                labels = sorted(set(l for m, l in sum([vote_detail[s] for s in range(bs, be)], [])))
                blocks.append((bs, be, f"{'/'.join(sorted(block_methods))}:{labels[0] if labels else 'ad'}"))
                log(f"  heatmap {bs:.0f}s-{be:.0f}s ({dur:.0f}s) votes={sorted(block_methods)}")
    if in_block:
        be = duration
        dur = be - bs
        if MIN_AD_LEN <= dur <= MAX_AD_LEN:
            labels = sorted(set(l for m, l in sum([vote_detail[s] for s in range(bs, be)], [])))
            blocks.append((bs, be, f"{'/'.join(sorted(block_methods))}:{labels[0] if labels else 'ad'}"))

    # Single-method high-precision: CTA, duplicate, cluster, brand, and
    # chapter are reliable alone. Voice, cluster-popular, and brand-orphan
    # alone have too many false positives — they only count via heat-map
    # 2+ votes.
    for s, e, label, method in candidates:
        if method in ('cta', 'duplicate', 'cluster', 'brand', 'chapter'):
            dur = e - s
            if not (MIN_AD_LEN <= dur <= MAX_AD_LEN):
                continue
            # Skip if already covered by a heatmap block
            if any(s < be and e > bs for bs, be, _ in blocks):
                continue
            # Skip duplicates (same label already added)
            if any(label == l for _, _, l in blocks):
                continue
            blocks.append((s, e, label))
            log(f"  single-method '{label}': {method} {s:.1f}s-{e:.1f}s ({dur:.0f}s)")

    # Log low-confidence cluster-popular hits for Nathan's review
    # (not auto-cut unless they got 2+ heat-map votes)
    for s, e, label, method in candidates:
        if method == 'cluster-popular':
            dur = e - s
            if not (MIN_AD_LEN <= dur <= MAX_AD_LEN):
                continue
            if any(s < be and e > bs for bs, be, _ in blocks):
                continue
            log(f"  LOW-CONFIDENCE '{label}': cluster-popular {s:.1f}s-{e:.1f}s ({dur:.0f}s) — not cut (single vote)")

    blocks.sort()
    return [(s, e, l) for s, e, l in blocks]


def _method_cta(words, sponsors, silences):
    """Method 'cta': Generic URL/promo-code CTA detection.
    Returns [(start, end, label)] independently."""
    nw = norm_words(words)
    results = []
    for brand, url_idx in find_url_mentions(nw):
        brand_variants = {brand, brand.replace(" ", "")}
        hits = find_mentions(nw, brand_variants)
        url_t = words[url_idx][0]
        hits = [h for h in hits if abs(words[h][0] - url_t) < 8 * 60]
        if not hits:
            hits = [url_idx]
        seg = _expand_segment(words, nw, silences, hits, brand)
        if seg:
            results.append((seg[0], seg[1], brand))
    return results


def _method_brand(words, sponsors, silences):
    """Method 'brand': Description sponsor brand mentions.
    Returns [(start, end, label)] independently."""
    nw = norm_words(words)
    results = []
    for sp in sponsors:
        hits = find_mentions(nw, sp["variants"])
        if not hits:
            continue
        seg = _expand_segment(words, nw, silences, hits, sp["label"])
        if seg:
            results.append((seg[0], seg[1], sp["label"]))
    return results


def _method_brand_orphan(words, sponsors, silences):
    """Method 'brand-orphan': sponsor mentions _expand_segment could not
    turn into a block (no opening cue found). Low-confidence: votes in the
    heat map but never cuts alone (like 'voice' and 'cluster-popular').
    Added 2026-10-14 per Nathan — Unsubscribe 268 showed DOSE/AG1 mentions
    being silently dropped this way. Window = mention span padded 45s each
    side, snapped to pauses, bounded by MIN/MAX_AD_LEN.
    Returns [(start, end, label)] independently."""
    nw = norm_words(words)
    pauses = get_pauses(words, silences)
    results = []
    for sp in sponsors:
        hits = find_mentions(nw, sp["variants"])
        if not hits:
            continue
        if _expand_segment(words, nw, silences, hits, sp["label"]):
            continue  # the full brand method already handles it
        times = sorted(words[i][0] for i in hits)
        start, end = max(0.0, times[0] - 45.0), times[-1] + 45.0
        # snap to nearby pauses, never into neighboring content
        for ps, pe in pauses:
            if ps >= start and ps - start <= 6.0:
                start = ps
                break
        for ps, pe in reversed(pauses):
            if pe <= end and end - pe <= 6.0:
                end = pe
                break
        if not (MIN_AD_LEN <= end - start <= MAX_AD_LEN):
            continue
        results.append((start, end, f"brand-orphan:{sp['label']}"))
        log(f"  brand-orphan '{sp['label']}': {start:.1f}s-{end:.1f}s "
            f"({len(hits)} mentions, no opening cue)")
    return results


def _method_cluster(words, sponsors, silences):
    """Method 'cluster': Dense brand-mention clusters (casual ad reads).
    Returns [(start, end, label)] independently.
    Threshold: 4+ mentions in 1 minute (tightened 2026-10-09 per Nathan;
    3-in-3min was cutting wanted content)."""
    nw = norm_words(words)
    results = []
    for sp in sponsors:
        hits = find_mentions(nw, sp["variants"])
        if not hits:
            continue
        times = [words[h][0] for h in hits]
        best = None
        for i in range(len(times)):
            j = i
            while j < len(times) and times[j] - times[i] <= 60:
                j += 1
            count = j - i
            if count >= 4 and (best is None or count > best[0]):
                best = (count, times[i], times[j - 1])
        if best:
            count, cs, ce = best
            seg_start = find_pause_before(words, silences, cs)
            seg_end = find_pause_after(words, silences, ce)
            results.append((seg_start, seg_end, sp["label"]))
    return results


def _method_cluster_popular(words, sponsors, silences):
    """Method 'cluster-popular': Dense mentions of popular sponsors NOT in
    the episode description (safety net for unlisted/dynamic ads).
    Same 4+/1min threshold. Tagged separately so the heat map treats
    single-vote hits as low-confidence (not auto-cut).
    Returns [(start, end, label)] independently."""
    # Skip brands already in the description sponsors (they're covered
    # by _method_cluster with high confidence)
    desc_labels = {sp["label"].lower() for sp in sponsors}
    popular = [pb for pb in POPULAR_BRANDS
               if pb["label"].lower() not in desc_labels]
    nw = norm_words(words)
    results = []
    for pb in popular:
        hits = find_mentions(nw, pb["variants"])
        if not hits:
            continue
        times = [words[h][0] for h in hits]
        best = None
        for i in range(len(times)):
            j = i
            while j < len(times) and times[j] - times[i] <= 60:
                j += 1
            count = j - i
            if count >= 4 and (best is None or count > best[0]):
                best = (count, times[i], times[j - 1])
        if best:
            count, cs, ce = best
            seg_start = find_pause_before(words, silences, cs)
            seg_end = find_pause_after(words, silences, ce)
            results.append((seg_start, seg_end, pb["label"]))
    return results


def _expand_segment(words, nw, silences, hits, label):
    """Expand hit indices to segment boundaries. Returns (start, end, kind) or None."""
    first, last = hits[0], hits[-1]
    seg_start = find_opening(words, nw, silences, first)
    if seg_start is None:
        produced = find_produced_spot(words, nw, hits, silences)
        if produced is None:
            return None
        return produced[0], produced[1], "produced spot"
    seg_end = find_closing(words, nw, silences, last, seg_start)
    if seg_end is None:
        return None
    return seg_start, seg_end, "host read"


def _method_voice(words, mp3_path):
    """Method 'voice': Host-voice anomaly detection (Nathan's idea 2026-10-09).
    Learn host voice profile, flag segments far from it.
    Returns [(start, end, label)]."""
    try:
        import numpy as np
        from python_speech_features import mfcc
        from sklearn.preprocessing import StandardScaler
        import wave
    except ImportError:
        log("  voice: python_speech_features not available — skipping")
        return []

    # Extract audio
    import tempfile
    with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as tf:
        wav_path = tf.name
    try:
        subprocess.run(['ffmpeg', '-y', '-v', 'error', '-i', mp3_path,
                        '-ar', '16000', '-ac', '1', wav_path], check=True)
        w = wave.open(wav_path)
        frames = w.readframes(w.getnframes())
        audio = np.frombuffer(frames, dtype=np.int16).astype(float)
        sr = w.getframerate()
        w.close()
    finally:
        if os.path.exists(wav_path):
            os.remove(wav_path)

    # 10s segments (more stable than 5s)
    seg_len = 10 * sr
    features = []
    for i in range(0, len(audio) - seg_len, seg_len):
        seg = audio[i:i+seg_len]
        m = mfcc(seg, samplerate=sr, numcep=13)
        features.append(m.mean(axis=0))
    if not features:
        return []
    features = np.array(features)

    # Host profile = median (robust)
    scaler = StandardScaler()
    fnorm = scaler.fit_transform(features)
    host_profile = np.median(fnorm, axis=0)
    dists = np.linalg.norm(fnorm - host_profile, axis=1)

    # Flag top 5% distance, require 15s+ contiguous
    threshold = np.percentile(dists, 95)
    results = []
    in_block = False
    bs = 0
    for i, d in enumerate(dists):
        t = i * 10
        if d > threshold and not in_block:
            in_block = True
            bs = t
        elif d <= threshold and in_block:
            in_block = False
            be = t
            if be - bs >= 15:
                results.append((bs, be, f"voice-anomaly"))
    if in_block:
        be = len(dists) * 10
        if be - bs >= 15:
            results.append((bs, be, f"voice-anomaly"))
    log(f"  voice: {len(results)} anomalous segments")
    return results


def find_duplicate_segments(words, min_shingle=8, min_gap=60, min_dur=15):
    """Find verbatim-repeated segments (inserted ads run 2+ times).
    Returns [(start, end, label)]."""
    from collections import defaultdict
    nw = norm_words(words)
    # Build shingles: 8 normalized words -> list of timestamps
    shingles = defaultdict(list)
    n = len(nw)
    for i in range(n - min_shingle):
        key = tuple(nw[i + k] for k in range(min_shingle))
        if all(key):
            shingles[key].append(words[i][0])
    # Find shingles repeated with >min_gap separation
    hits = []  # (time,)
    for key, times in shingles.items():
        if len(times) >= 2 and max(times) - min(times) > min_gap:
            hits.extend(times)
    if not hits:
        return []
    # Cluster hit times into segments (within 40s = same ad)
    hits.sort()
    clusters = []
    cs, ce = hits[0], hits[0]
    for t in hits[1:]:
        if t - ce < 40:
            ce = t
        else:
            clusters.append((cs, ce))
            cs, ce = t, t
    clusters.append((cs, ce))
    # Expand each cluster to cover the full repeated phrase (+15s each side
    # for the ad boundaries) and label
    result = []
    for i, (cs, ce) in enumerate(clusters):
        dur = ce - cs
        if dur < 5:  # too short, likely coincidence
            continue
        # Expand by 20s each side to catch full ad
        ds = max(0, cs - 20)
        de = ce + 20
        if de - ds >= min_dur:
            result.append((ds, de, f"duplicate-{i+1}"))
    return result


def find_pause_before(words, silences, t):
    """Nearest silence start before time t (for clean cut boundaries)."""
    best = None
    for ss, se in silences:
        if se <= t and (best is None or se > best):
            best = se
    # fall back to word boundary 2s before
    if best is None:
        best = max(0, t - 2)
    return best


def find_pause_after(words, silences, t):
    """Nearest silence end after time t (for clean cut boundaries)."""
    best = None
    for ss, se in silences:
        if ss >= t and (best is None or ss < best):
            best = ss
    if best is None:
        best = t + 2
    return best


def detect_narrative_breaks(words, silences):
    """Find ad blocks via SEMANTIC topical discontinuity.

    Uses sentence embeddings to detect "jarring narrative jumps":
    window A (topic X) -> window B (unrelated) -> window C (topic X again).
    Returns [(start, end, label)].
    """
    import os
    # workaround: httpx misparses the no_proxy var's IPv6 brackets
    os.environ.pop('no_proxy', None)
    os.environ.pop('NO_PROXY', None)

    try:
        from sentence_transformers import SentenceTransformer
        import numpy as np
    except ImportError:
        log("  narrative: sentence-transformers not available — skipping")
        return []

    WIN = 30
    if not words:
        return []
    total = words[-1][0] + 2
    nwin = int(total / WIN) + 1

    # build window texts
    win_texts = [""] * nwin
    for w in words:
        idx = int(w[0] / WIN)
        if idx < nwin:
            win_texts[idx] += w[2] + " "
    win_texts = [t.strip() for t in win_texts]
    # filter empty
    valid = [i for i, t in enumerate(win_texts) if len(t.split()) >= 10]
    if len(valid) < 3:
        return []

    log("  narrative: loading embedding model...")
    model = SentenceTransformer('all-MiniLM-L6-v2')
    texts = [win_texts[i] for i in valid]
    embs = model.encode(texts, show_progress_bar=False)
    # cosine similarity matrix
    norms = np.linalg.norm(embs, axis=1, keepdims=True)
    norms = np.where(norms == 0, 1, norms)
    embs_n = embs / norms
    sim = embs_n @ embs_n.T

    # map valid-index -> position in sim matrix
    blocks = []
    for k in range(1, len(valid) - 1):
        i_prev, i_cur, i_next = valid[k-1], valid[k], valid[k+1]
        # require consecutive windows (no gaps)
        if not (i_cur == i_prev + 1 and i_next == i_cur + 1):
            continue
        sab = sim[k-1, k]
        sbc = sim[k, k+1]
        # B semantically distant from BOTH neighbors.
        # (The "return" pattern doesn't hold in meandering conversation;
        #  the ad is simply a semantic island.)
        if sab < 0.35 and sbc < 0.35:
            start = i_prev * WIN
            # extend while disconnected
            j = k
            while j + 1 < len(valid) - 1:
                jk, jk1 = valid[j], valid[j+1]
                if jk1 != jk + 1:
                    break
                if sim[j, j+1] < 0.40:
                    j += 1
                else:
                    break
            end = (valid[j] + 1) * WIN
            dur = end - start
            if 45 <= dur <= 720:
                ps = find_pause_before(words, silences, start + 5)
                pe = find_pause_after(words, silences, end - 5)
                blocks.append((ps, pe, f"semantic-break@{int(start)}s"))
                log(f"  narrative: {ps:.0f}s-{pe:.0f}s "
                    f"(sim in/out {sab:.2f}/{sbc:.2f})")
    return blocks


def _is_ad_chapter_title(title):
    """Does a chapter title label an ad segment?"""
    t = title.strip()
    return any(re.search(p, t, re.I) for p in CHAPTER_AD_PATTERNS)


def _merge_chapters(id3_chapters, rss_chapters):
    """Combine ID3 and RSS chapter lists, deduping near-identical starts
    so one ad chapter doesn't vote twice."""
    merged = list(id3_chapters)
    for sec, title in rss_chapters:
        if not any(abs(sec - s) < 2.0 for s, _ in merged):
            merged.append((sec, title))
    return sorted(merged)


def _method_chapter(words, chapters, silences):
    """Method 'chapter': publisher-labeled ad chapters (2026-10-14).

    Chapters titled like '[Ad]', 'Ad:', 'Advertisement', 'Sponsor' are
    high-precision ad markers. Span = chapter start -> next chapter start
    (or episode end), snapped to nearby pauses for clean cuts.
    Returns [(start, end, label)] independently.
    """
    if not chapters:
        return []
    duration = words[-1][0]
    pauses = get_pauses(words, silences)
    results = []
    ordered = sorted(chapters)
    for i, (start, title) in enumerate(ordered):
        if not _is_ad_chapter_title(title):
            continue
        end = ordered[i + 1][0] if i + 1 < len(ordered) else duration
        # Snap to nearby pauses for clean cuts, never into neighboring
        # content: start snaps forward, end snaps backward (<=4s each).
        for ps, pe in pauses:
            if ps >= start and ps - start <= 4.0:
                start = ps
                break
        for ps, pe in reversed(pauses):
            if pe <= end and end - pe <= 4.0:
                end = pe
                break
        if end - start < 5:
            continue
        results.append((start, end, f"chapter:{title.strip()[:40]}"))
        log(f"  chapter marker '{title.strip()[:40]}': {start:.1f}s-{end:.1f}s")
    return results


def find_url_mentions(nw):
    """Find ad-like URL/CTA mentions: [(brand, word_index)].

    An ad anchor requires BOTH a call-to-action phrase AND a nearby URL.
    This filters out URLs mentioned in normal content.
    """
    tlds = {"com", "net", "org", "io", "tv", "co", "ai", "me"}
    cta_phrases = [
        ("go", "to"), ("visit",), ("check", "out"), ("head", "to"),
        ("head", "over", "to"), ("give", "it", "a", "try", "at"),
        ("try", "at"), ("sign", "up", "at"), ("get", "it", "at"),
        ("available", "at"),
    ]
    L = len(nw)
    # index URL positions: word_index -> brand
    url_at = {}
    for i in range(L):
        w = nw[i]
        brand = None
        if w == "dot" and i + 1 < L and nw[i + 1].rstrip(".,") in tlds:
            parts = []
            for j in (i - 1, i - 2):
                if j < 0:
                    break
                pw = nw[j].strip(".,")
                if not re.fullmatch(r"[a-z0-9'-]+", pw) or pw in (
                        "to", "at", "go", "visit", "our", "my", "the", "a"):
                    break
                parts.append(pw)
            parts.reverse()
            if parts:
                brand = "".join(parts)
        elif w.startswith(".") and w[1:].rstrip(".,") in tlds and i > 0:
            pw = nw[i - 1].strip(".,")
            if re.fullmatch(r"[a-z0-9'-]{2,}", pw) and pw not in (
                    "our", "my", "the", "website", "site"):
                brand = pw
        if brand and brand not in ("website", "site", "page"):
            url_at[i] = brand
    out = []
    # promo codes are standalone CTAs
    for i in range(L):
        if nw[i] == "code" and i + 1 < L:
            if (i >= 1 and nw[i - 1] in ("use", "promo")) or \
               (i >= 2 and nw[i - 2] == "promo"):
                out.append((f"code-{nw[i + 1]}", i + 1))
    # CTA + nearby URL = ad anchor
    for i in range(L):
        for phrase in cta_phrases:
            n = len(phrase)
            if i + n < L and tuple(nw[i:i + n]) == phrase:
                for j in range(i + n, min(i + n + 15, L)):
                    if j in url_at:
                        out.append((url_at[j], j))
                        break
                break
    # de-dupe by brand, keep earliest index
    seen = {}
    for brand, idx in out:
        if brand not in seen:
            seen[brand] = idx
    return [(b, seen[b]) for b in seen]


def sponsor_root(label):
    """Distinctive word from a sponsor label, e.g. 'material.security' -> 'material'."""
    t = re.sub(r"^https?://(www\.)?", "", label.lower()).split("/")[0]
    t = re.sub(r"\.(com|net|org|io|ai|tv|co|us|security)$", "", t)
    return t.split()[0] if t else ""


def check_missed_sponsors(words, sponsors, blocks):
    """Sponsors from the description with no cut block. Returns
    [{label, mentions}] so nothing is silently skipped.

    A sponsor counts as covered when any of its transcript mentions
    (fuzzy-matched, same as detection) falls inside a cut block, or a
    block label names it. Only genuinely uncut mentions are reported —
    2026-10-12: exact-label matching falsely reported Chime/Quince as
    missed on Lore Legends 90 when their reads were inside a merged block.
    """
    nw = norm_words(words)
    missed = []
    for sp in sponsors:
        variants = {v for v in sp.get("variants", set()) if v and len(v) >= 4}
        if not variants:
            continue
        hits = find_mentions(nw, variants)
        if not hits:
            log(f"  WARNING: sponsor '{sp['label']}' not in audio (0 transcript mentions)")
            missed.append({"label": sp["label"], "mentions": 0})
            continue
        hit_times = [words[i][0] for i in hits]
        covered = any(s <= t <= e for t in hit_times for s, e, _ in blocks)
        if not covered:
            covered = any(sp["label"].lower() in (lbl or "").lower()
                          for _, _, lbl in blocks)
        if not covered:
            log(f"  WARNING: sponsor '{sp['label']}' not cut "
                f"({len(hits)} transcript mentions)")
            missed.append({"label": sp["label"], "mentions": len(hits)})
    return missed


def read_chapters(mp3_path):
    """Existing ID3 chapter markers. Returns [(start_ms, title)]."""
    from mutagen.mp3 import MP3
    try:
        audio = MP3(mp3_path)
    except Exception:
        return []
    if not audio.tags:
        return []
    chaps = []
    for key in audio.tags:
        if key.startswith("CHAP:"):
            c = audio.tags[key]
            title = ""
            if "TIT2" in c.sub_frames:
                title = str(c.sub_frames["TIT2"].text[0])
            chaps.append((c.start_time, title))
    return sorted(chaps)


def remap_chapters(chapters, blocks):
    """Shift chapter timestamps for removed ad blocks.

    chapters: [(start_ms, title)]. blocks: [(start_s, end_s, label)].
    Chapters inside a removed block are dropped.
    """
    out = []
    for start_ms, title in chapters:
        s = start_ms / 1000.0
        if any(bs <= s < be for bs, be, _ in blocks):
            log(f"  chapter '{title}' was inside a removed ad block — dropped")
            continue
        shift = sum(be - bs for bs, be, _ in blocks if be <= s)
        out.append((int((s - shift) * 1000), title))
    return out


def write_chapters(mp3_path, chapters, duration_s):
    """Embed chapter markers. chapters: [(start_ms, title)]."""
    from mutagen.mp3 import MP3
    from mutagen.id3 import CHAP, CTOC, TIT2
    audio = MP3(mp3_path)
    if audio.tags is None:
        audio.add_tags()
    # remove any existing chapters
    for key in [k for k in audio.tags if k.startswith("CHAP:") or k.startswith("CTOC:")]:
        del audio.tags[key]
    ids = []
    for i, (start_ms, title) in enumerate(chapters):
        eid = f"ch{i:02d}"
        end_ms = chapters[i + 1][0] if i + 1 < len(chapters) else int(duration_s * 1000)
        audio.tags.add(CHAP(element_id=eid, start_time=start_ms, end_time=end_ms,
                            start_offset=0, end_offset=0,
                            sub_frames=[TIT2(encoding=3, text=title)]))
        ids.append(eid)
    if ids:
        audio.tags.add(CTOC(element_id="toc", flags=3, child_element_ids=ids,
                            sub_frames=[TIT2(encoding=3, text="Chapters")]))
    audio.save()
    log(f"  wrote {len(ids)} chapters to {os.path.basename(mp3_path)}")


def generate_chapters(words, duration):
    """Auto-generate chapters from topic changes in the transcript.

    Splits at the largest pauses, targeting ~10 minute segments, and
    titles each from its most distinctive words. Returns [(start_ms, title)].
    """
    if not words or duration < 600:
        return []
    # candidate boundaries: pauses between words, largest first
    pauses = []
    for i in range(1, len(words)):
        gap = words[i][0] - words[i - 1][1]
        if gap >= 1.5:
            pauses.append((gap, words[i][0]))
    pauses.sort(reverse=True)
    # target ~1 chapter per 10 min, min 3, max 15
    n_target = max(3, min(15, int(duration / 600)))
    bounds = sorted({0.0} | {t for _, t in pauses[:n_target - 1]} | {duration})
    # merge segments shorter than 3 min into the next
    merged = [bounds[0]]
    for b in bounds[1:]:
        if b - merged[-1] < 180 and b != duration:
            continue
        merged.append(b)
    if len(merged) < 2:
        return []
    # word frequencies for the whole episode (for distinctiveness)
    stop = set("""the a an and or of to in on for with is are was were be been
        it its this that these those i you he she we they them his her our your
        as at by from but not no yes so if then than when where what which who
        whom whose how all any can could would should will shall may might must
        just very really much more most many some such only even still also
        out up down over under again once here there now today tonight well oh
        uh um yeah okay ok right sure like get got gets getting going go goes
        went come comes came know knows knew think thinks thought see sees saw
        look looks looked say says said tell tells told talk talks talked ask
        asked want wants wanted need needs needed take takes took make makes
        made use uses used do does did done have has had having thing things
        something anything everything nothing someone anyone everyone people
        one ones two three first second new old big small large little great
        good bad well better best much many lot lots kind sort type way ways
        part parts bit time times year years day days week weeks month months
        today tonight now then here there where when why how what which who
        don doesn didn isn aren wasn weren hasn haven hadn won wouldn couldn
        shouldn ll ve re m s t""".split())

    def clean_word(w):
        t = re.sub(r"[^a-z']", "", w.lower())
        # strip contractions: that's -> that, we're -> we, don't -> don
        t = re.sub(r"'(s|re|t|ll|ve|d|m)$", "", t)
        t = t.strip("'")
        return t

    total = {}
    seg_words = []
    for i in range(len(merged) - 1):
        s, e = merged[i], merged[i + 1]
        toks = [clean_word(w[2]) for w in words if s <= w[0] < e]
        toks = [t for t in toks if t and t not in stop and len(t) >= 4]
        seg_words.append(toks)
        for t in set(toks):
            total[t] = total.get(t, 0) + 1
    chapters = []
    for i, toks in enumerate(seg_words):
        if not toks:
            chapters.append((int(merged[i] * 1000), f"Part {i + 1}"))
            continue
        # Title from the most frequent content-word bigram — far more
        # topical than single words ("blind signature" vs "signature").
        from collections import Counter
        bigrams = Counter()
        for j in range(len(toks) - 1):
            a, b = toks[j], toks[j + 1]
            # skip stutters and short generic words
            if a != b and len(a) >= 5 and len(b) >= 5:
                bigrams[a + " " + b] += 1
        title = f"Part {i + 1}"
        if bigrams:
            bg, count = bigrams.most_common(1)[0]
            if count >= 3:
                title = " ".join(w.capitalize() for w in bg.split())
        chapters.append((int(merged[i] * 1000), title))
    return chapters


def build_ads_reel(mp3_path, blocks, out_path, work_dir):
    """Concatenate cut ad segments in chronological order into a QA reel.
    Nathan listens to verify we're not cutting wanted content.
    Each segment gets its own chapter; clip list with timestamps goes
    in the feed item description (added by write_feed)."""
    if not blocks:
        return
    seg_files = []
    sorted_blocks = sorted(blocks)
    for i, (s, e, label) in enumerate(sorted_blocks):
        seg = os.path.join(work_dir, f"adseg_{i:02d}.mp3")
        subprocess.run(
            ["ffmpeg", "-y", "-v", "error", "-i", mp3_path,
             "-ss", f"{s:.2f}", "-to", f"{e:.2f}",
             "-c:a", "libmp3lame", "-b:a", "128k", seg], check=True)
        seg_files.append(seg)
    # Concatenate
    concat_list = os.path.join(work_dir, "ads_concat.txt")
    with open(concat_list, "w") as f:
        for seg in seg_files:
            f.write(f"file '{seg}'\n")
    tmp_out = os.path.join(work_dir, "ads_reel_tmp.mp3")
    subprocess.run(
        ["ffmpeg", "-y", "-v", "error", "-f", "concat", "-safe", "0",
         "-i", concat_list, "-c", "copy", tmp_out], check=True)
    # Add chapters, one per segment
    chap_time = 0
    meta = ";FFMETADATA1\n"
    for s, e, label in sorted_blocks:
        dur = e - s
        start_ms = int(chap_time * 1000)
        chap_time += dur
        end_ms = int(chap_time * 1000)
        # Sanitize label for metadata
        safe_label = re.sub(r"[^\w\s\-()]", "", str(label))[:80]
        meta += f"[CHAPTER]\nTIMEBASE=1/1000\nSTART={start_ms}\nEND={end_ms}\ntitle={safe_label}\n"
    meta_path = os.path.join(work_dir, "reel_chapters.txt")
    with open(meta_path, "w") as f:
        f.write(meta)
    subprocess.run(
        ["ffmpeg", "-y", "-v", "error", "-i", tmp_out, "-i", meta_path,
         "-map_metadata", "1", "-c", "copy", out_path], check=True)
    for sf in seg_files + [tmp_out, concat_list, meta_path]:
        if os.path.exists(sf):
            os.remove(sf)


def cut_with_whoosh(mp3_path, blocks, out_path, work_dir, duration):
    """Cut ad blocks, inserting whoosh.wav at each splice."""
    keep = []
    cursor = 0.0
    for s, e, _ in sorted(blocks):
        if s > cursor + 0.5:
            keep.append((cursor, s))
        cursor = max(cursor, e)
    if cursor < duration - 0.5:
        keep.append((cursor, duration))

    seg_files = []
    inputs = []
    for i, (s, e) in enumerate(keep):
        seg = os.path.join(work_dir, f"keep_{i:02d}.wav")
        subprocess.run(
            ["ffmpeg", "-y", "-v", "error", "-i", mp3_path,
             "-ss", f"{s:.2f}", "-to", f"{e:.2f}",
             "-ar", "44100", "-ac", "2", seg], check=True)
        seg_files.append(seg)
        inputs += ["-i", seg, "-i", WHOOSH_PATH]

    # chain acrossfades: keep0 ~ whoosh ~ keep1 ~ whoosh ~ keep2 ...
    fc = []
    labels = []
    n = len(seg_files)
    # input index layout: 0=keep0, 1=whoosh, 2=keep1, 3=whoosh, ...
    prev = "0:a"
    step = 0
    for i in range(n):
        w_idx = 2 * i + 1
        if i < n - 1:
            out_l = f"x{step}"
            fc.append(f"[{prev}][{w_idx}:a]acrossfade=d=0.1:c1=tri:c2=tri[{out_l}]")
            prev = out_l
            step += 1
            out_l2 = f"x{step}"
            fc.append(f"[{prev}][{2 * (i + 1)}:a]acrossfade=d=0.1:c1=tri:c2=tri[{out_l2}]")
            prev = out_l2
            step += 1
    cmd = ["ffmpeg", "-y", "-v", "error"] + inputs + [
        "-filter_complex", ";".join(fc), "-map", f"[{prev}]",
        "-c:a", "libmp3lame", "-b:a", "128k", out_path]
    subprocess.run(cmd, check=True)
    for f in seg_files:
        os.remove(f)
    return keep


def verify_splices(mp3_path, blocks, sponsors, work_dir, keep):
    """Re-transcribe around each splice; fail if ad content leaks.

    The swish itself is excluded (±1.5s): whisper hallucinates words
    from non-speech audio, which caused false brand-word flags.
    """
    from faster_whisper import WhisperModel
    model = load_model()
    brand_words = set()
    for sp in sponsors:
        brand_words.update(sp["variants"])
    problems = []
    w_net = probe_duration(WHOOSH_PATH) - 0.2  # swish minus 2x0.1s crossfade
    out_pos = 0.0
    for i, (s, e) in enumerate(keep[:-1]):
        # output position of splice i: kept audio so far + net per inserted swish
        out_pos += (e - s) + w_net
        splice_at = out_pos - w_net
        for tag, c0 in (("pre", max(0, splice_at - 14)), ("post", splice_at + 2)):
            clip = os.path.join(work_dir, f"splice_{i}_{tag}.wav")
            subprocess.run(
                ["ffmpeg", "-y", "-v", "error", "-ss", f"{c0:.2f}", "-t", "12",
                 "-i", mp3_path, "-ar", "16000", "-ac", "1", clip], check=True)
            segments, _ = transcribe_file(model, clip, word_timestamps=False)
            txt = " ".join(seg.text for seg in segments).lower()
            os.remove(clip)
            hits = [b for b in brand_words if b and len(b) >= 4 and b in txt]
            cues = [c for c in OPEN_CUES if c in txt]
            if hits or cues:
                problems.append((i, tag, hits, cues, txt[:160]))
    return problems


def format_duration(seconds):
    s = int(round(seconds))
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def edit_summary_html(ep):
    blocks = ep.get("blocks", [])
    total = sum(b["seconds"] for b in blocks)
    n = len(blocks)
    # Plain text edits report (per Nathan 2026-10-09) — podcast apps
    # expect plain text descriptions, HTML tags break them.
    # Report goes at TOP so it's visible without scrolling.
    lines = [
        "———————————————————",
        "",
        f"Edits made: removed {n} ad segment{'s' if n != 1 else ''} "
        f"totaling {total / 60:.1f} minutes.",
        "",
    ]
    for b in blocks:
        mins = b["seconds"] / 60
        lines.append(f"- {b['sponsor']}: {mins:.1f} min")
    lines.append("")
    lines.append("A short swish sound marks each edit.")
    # Escape the title: descriptions are inserted as raw XML text (not CDATA)
    # when the source description isn't CDATA — a bare & would break the feed.
    # (Same fix as reel titles, 2026-10-09.)
    safe_title = ep.get("title", "").strip().replace("&", "&amp;")
    lines.append("A QA reel with all removed segments is available as "
                 f"\"Ads cut from {safe_title}\" in this feed.")
    lines.append("")
    lines.append("")
    return "\n".join(lines)


def write_feed(state, site_dir, original_xml):
    """Mirror the original feed: identical channel metadata, artwork,
    titles and descriptions — except cleaned episodes get the cleaned
    enclosure URL and an edit summary appended to the description."""
    base = state.get("base_url", "").rstrip("/")
    cleaned = {g: ep for g, ep in state["processed"].items()
               if not ep.get("skipped") and ("file" in ep or "enclosure_url" in ep)}

    def repl_item(m):
        body = m.group(1)
        gm = re.search(r"<guid[^>]*>(.*?)</guid>", body, re.S)
        guid = unescape(gm.group(1)).strip() if gm else ""
        # Strip CDATA wrapper if present (html.unescape doesn't handle it)
        if guid.startswith("<![CDATA[") and guid.endswith("]]>"):
            guid = guid[9:-3].strip()
        ep = cleaned.get(guid)
        if not ep:
            return m.group(0)
        # Direct enclosure URL (e.g. remote-storage link) takes precedence;
        # falls back to base_url + filename for artifact-hosted files.
        if ep.get("enclosure_url"):
            new_url = ep["enclosure_url"]
        elif base and ep.get("file"):
            new_url = base + "/" + ep["file"]
        else:
            return m.group(0)
        body = re.sub(
            r'<enclosure[^>]*>',
            f'<enclosure url="{new_url}" length="{ep.get("enclosure_bytes", ep["bytes"])}" type="audio/mpeg"/>',
            body, count=1)
        if "<itunes:duration>" in body and ep.get("cleaned_seconds"):
            body = re.sub(
                r"<itunes:duration>.*?</itunes:duration>",
                f"<itunes:duration>{format_duration(ep['cleaned_seconds'])}</itunes:duration>",
                body, count=1)
        dm = re.search(r"(<description>)(.*?)(</description>)", body, re.S)
        if dm:
            # Handle both CDATA and plain descriptions
            # Edits report goes at TOP (per Nathan 2026-10-09) so it's visible
            inner = dm.group(2)
            html = edit_summary_html(ep)
            if inner.startswith("<![CDATA["):
                new_desc = dm.group(1) + "<![CDATA[" + html + inner[9:-3] + "]]>" + dm.group(3)
            else:
                new_desc = dm.group(1) + html + inner + dm.group(3)
            body = body[:dm.start()] + new_desc + body[dm.end():]
        return "<item>" + body + "</item>"

    feed = re.sub(r"<item>(.*?)</item>", repl_item, original_xml, flags=re.S)
    # Insert ads-reel QA items after their main episodes.
    # Title: "Ads cut from (episode name)" so Nathan can verify cuts.
    for guid, ep in cleaned.items():
        if not ep.get("ads_reel_url"):
            continue
        # Reel guid: strip any CDATA wrapper baked into the state key so the
        # parsed guid is unique per episode. (2026-10-09: two reels shared the
        # parsed guid "c1f745dc-...-ads-reel" because one key carried 284's
        # guid and the other wrapped it in CDATA — podcast apps dedupe by
        # guid, so the reels never appeared.)
        reel_guid = re.sub(r"<!\[CDATA\[(.*?)\]\]>", r"\1", guid) + "-ads-reel"
        # Escape XML special chars in title (per Nathan 2026-10-09 bug fix)
        raw_title = f"Ads cut from {ep['title']}"
        reel_title = (raw_title.replace("&", "&amp;")
                               .replace("<", "&lt;")
                               .replace(">", "&gt;"))
        # Build timestamped clip list from blocks
        clips = []
        t = 0
        for b in ep.get("blocks", []):
            m, s = divmod(int(t), 60)
            sponsor = b['sponsor']
            clips.append(f"{m}:{s:02d} - {sponsor} ({b['seconds']:.0f}s)")
            t += b["seconds"]
        clip_list = "\n".join(clips)
        # Edits report (same as main episode, per Nathan 2026-10-09)
        # Plain text — podcast apps expect plain text descriptions
        blocks = ep.get("blocks", [])
        total = sum(b["seconds"] for b in blocks)
        n = len(blocks)
        edits_lines = [
            "———————————————————",
            "",
            f"Edits made: removed {n} ad segment{'s' if n != 1 else ''} "
            f"totaling {total / 60:.1f} minutes.",
            "",
        ]
        for b in blocks:
            mins = b["seconds"] / 60
            edits_lines.append(f"- {b['sponsor']}: {mins:.1f} min")
        edits_report = "\n".join(edits_lines)
        # iTunes fields required by podcast apps (Podcast Addict filters
        # items without them). Duration from reel blocks total.
        reel_secs = int(sum(b["seconds"] for b in blocks))
        reel_dur = f"{reel_secs // 3600}:{(reel_secs % 3600) // 60:02d}:{reel_secs % 60:02d}"
        reel_desc = (
            f"QA reel: ad segments removed from \"{ep['title']}\". "
            f"Listen to verify no wanted content was cut.\n\nClips:\n{clip_list}"
            f"\n\n{edits_report}"
        )
        # Escape for CDATA (can't contain ]]>)
        reel_desc = reel_desc.replace("]]>", "]]&gt;")
        reel_item = (
            f"<item><title>{reel_title}</title>"
            f"<description><![CDATA[{reel_desc}]]></description>"
            f"<enclosure url=\"{ep['ads_reel_url']}\" length=\"{ep.get('ads_reel_bytes', 0)}\" type=\"audio/mpeg\"/>"
            f"<guid isPermaLink=\"false\">{reel_guid}</guid>"
            f"<pubDate>{ep.get('pubDate', '')}</pubDate>"
            f"<itunes:duration>{reel_dur}</itunes:duration>"
            f"<itunes:episodeType>bonus</itunes:episodeType>"
            f"<itunes:explicit>yes</itunes:explicit>"
            f"</item>\n"
        )
        # Insert after the main episode's item using string search
        # (regex was crossing item boundaries). Anchor on the guid element's
        # closing tag so a guid that is a substring of another item's guid
        # can't match the wrong item (2026-10-09: the 283 reel was inserted
        # after the 284 item for exactly this reason).
        guid_marker = guid  # the guid string as it appears in the feed
        # Find the </item> that closes the item containing this guid
        guid_pos = feed.find(guid_marker + "</guid>")
        if guid_pos == -1:
            guid_pos = feed.find(guid_marker)
        if guid_pos != -1:
            # Find the next </item> after the guid
            item_end = feed.find("</item>", guid_pos)
            if item_end != -1:
                insert_pos = item_end + len("</item>")
                feed = feed[:insert_pos] + "\n" + reel_item + feed[insert_pos:]
    # Point the feed's self-identifier at our copy, or podcast apps revert
    # the subscription URL to the original feed address.
    feed = re.sub(
        r'<atom:link[^>]*rel="self"[^>]*>',
        f'<atom:link href="{GITHUB_FEED_URL}" type="application/rss+xml" rel="self"/>',
        feed, count=1)
    # Strip the permanent-redirect tag copied from the original feed, or apps
    # follow it straight back to the original address.
    feed = re.sub(r'\s*<itunes:new-feed-url>.*?</itunes:new-feed-url>', '', feed, count=1)
    feed = re.sub(
        r'(<rawvoice:subscribe feed=")[^"]*(")',
        f'\\1{GITHUB_FEED_URL}\\2',
        feed, count=1)
    os.makedirs(site_dir, exist_ok=True)
    with open(os.path.join(site_dir, "feed.xml"), "w") as f:
        f.write(feed)
    return feed


def process_episode(item, state):
    slug = re.sub(r"[^a-z0-9]+", "-", item["title"].lower()).strip("-")[:60]
    work = os.path.join(EP_DIR, slug)
    os.makedirs(work, exist_ok=True)
    mp3_path = os.path.join(work, "original.mp3")
    download(item["audio_url"], mp3_path)

    log(f"transcribing {item['title']}")
    words, duration = transcribe_chunked(mp3_path, work)
    log(f"transcript: {len(words)} words, {duration:.0f}s audio")

    sponsors = extract_sponsors(item["description"])
    sponsors += [dict(p) for p in HOUSE_PLUGS]
    log(f"sponsors from RSS: {[s['label'] for s in sponsors]}")
    # Always run detection: CTA/duplicate/voice/cluster-popular methods are
    # sponsor-independent (2026-10-12: skipping them for sponsor-less
    # episodes meant Legends 90 was published without any ad check).
    silences = find_silences(mp3_path, work, min_dur=0.8)
    # Chapter-marker signal (6th detection method, 2026-10-14): publisher-
    # labeled ad chapters from ID3 tags and RSS psc:chapters.
    existing_ms = read_chapters(mp3_path)
    id3_chapters = [(ms / 1000.0, t) for ms, t in existing_ms]
    chapters = _merge_chapters(id3_chapters, item.get("rss_chapters", []))
    if chapters:
        log(f"{len(chapters)} chapters available for chapter-marker detection")
    blocks = detect_ad_blocks(words, sponsors, silences, mp3_path, chapters)
    missed = check_missed_sponsors(words, sponsors, blocks)
    if not sponsors:
        log("no sponsors listed in description; sponsor-independent detection only")

    out_name = f"{slug}-cleaned.mp3"
    out_path = os.path.join(SITE_DIR, out_name)
    if blocks:
        log(f"cutting {len(blocks)} ad blocks with whoosh transitions")
        keep = cut_with_whoosh(mp3_path, blocks, out_path, work, duration)
        problems = verify_splices(out_path, blocks, sponsors, work, keep)
        if problems:
            log(f"WARNING: splice verification issues: {problems}")
    else:
        log("no confident ad blocks; copying episode whole")
        subprocess.run(["ffmpeg", "-y", "-v", "error", "-i", mp3_path,
                        "-c", "copy", out_path], check=True)
        keep = [(0, duration)]

    cleaned_dur = probe_duration(out_path)
    removed = duration - cleaned_dur
    # Build ads-reel: cut segments pasted together chronologically for QA.
    # Nathan listens to verify we're not cutting content he wants to keep.
    ads_reel_name = None
    if blocks:
        ads_reel_name = f"{slug}-ads-cut.mp3"
        ads_reel_path = os.path.join(SITE_DIR, ads_reel_name)
        build_ads_reel(mp3_path, blocks, ads_reel_path, work)
        log(f"ads-reel: {ads_reel_name} ({len(blocks)} segments)")
    # Chapters: remap pre-existing ones for the cuts, or auto-generate.
    # (words are in the original timeline; remap shifts them to cleaned time.)
    # Reuses the ID3 chapters read before detection (no second MP3 scan).
    existing = existing_ms
    if existing:
        log(f"remapping {len(existing)} pre-existing chapters for {len(blocks)} cuts")
        chapters = remap_chapters(existing, blocks)
    else:
        log("no pre-existing chapters; auto-generating from transcript")
        chapters = remap_chapters(
            [(ms, t) for ms, t in generate_chapters(words, duration)], blocks)
    if chapters:
        write_chapters(out_path, chapters, cleaned_dur)
    # phone-friendly copy for the chat link (kept out of the feed site)
    small_name = out_name.replace("-cleaned.mp3", "-cleaned-small.mp3")
    small_path = os.path.join(work, small_name)
    subprocess.run(["ffmpeg", "-y", "-v", "error", "-i", out_path,
                    "-c:a", "libmp3lame", "-b:a", "64k", small_path], check=True)
    report = {
        "title": item["title"], "pubDate": item["pubDate"],
        "original_seconds": duration, "cleaned_seconds": cleaned_dur,
        "removed_seconds": removed,
        "blocks": [{"start": s, "end": e, "sponsor": l} for s, e, l in blocks],
        "missed_sponsors": missed,
    }
    with open(os.path.join(work, "report.json"), "w") as f:
        json.dump(report, f, indent=2)

    state["processed"][item["guid"]] = {
        "title": item["title"], "pubDate": item["pubDate"],
        "file": out_name, "small_file": os.path.relpath(small_path, BASE_DIR),
        "bytes": os.path.getsize(out_path),
        "ads_removed": len(blocks), "ads_seconds": removed,
        "cleaned_seconds": cleaned_dur,
        "blocks": [{"sponsor": l, "seconds": e - s} for s, e, l in blocks],
        "missed_sponsors": [m["label"] for m in missed],
        "ads_reel": ads_reel_name,
        "ads_reel_bytes": os.path.getsize(os.path.join(SITE_DIR, ads_reel_name)) if ads_reel_name else 0,
    }
    save_state(state)
    log(f"done: {item['title']} — removed {removed:.0f}s in {len(blocks)} blocks")
    return report


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--podcast", required=True,
                    help="podcast id from podcasts.yaml")
    ap.add_argument("--limit", type=int, default=0,
                    help="process at most N newest unprocessed episodes (0 = no limit)")
    args = ap.parse_args()

    cfg, pcfg = init_podcast(args.podcast)
    log(f"podcast: {pcfg['name']} ({args.podcast})")
    state = load_state()
    if not os.path.exists(WHOOSH_PATH):
        log(f"FATAL: whoosh file missing at {WHOOSH_PATH}")
        sys.exit(1)
    xml = fetch_feed()
    items = parse_items(xml)
    log(f"feed has {len(items)} items")
    new_items = [it for it in reversed(items)
                 if it["guid"] and it["guid"] not in state["processed"] and it["audio_url"]]
    if args.limit:
        new_items = new_items[-args.limit:]
    if not new_items:
        log("no new episodes")
        return
    for item in new_items:
        log(f"processing: {item['title']}")
        try:
            process_episode(item, state)
        except Exception as exc:
            log(f"FAILED on {item['title']}: {exc!r}")
            # leave unprocessed so a later run retries
    write_feed(state, SITE_DIR, xml)
    log("feed regenerated")


if __name__ == "__main__":
    main()
