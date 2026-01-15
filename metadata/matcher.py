import os
import re

from rapidfuzz import fuzz
from mutagen import File as MutagenFile

_TITLE_CLEAN_RE = re.compile(
    r"\s*[\(\[\{][^)\]\}]*?(official|music video|video|lyric|audio|visualizer|full video|hd|4k)[^)\]\}]*?[\)\]\}]\s*",
    re.IGNORECASE,
)
_TITLE_TRAIL_RE = re.compile(
    r"\s*-\s*(official|music video|video|lyric|audio|visualizer|full video).*$",
    re.IGNORECASE,
)
_VEVO_SUFFIX_RE = re.compile(r"(vevo)$", re.IGNORECASE)


def parse_source(meta, file_path):
    title = _clean_title((meta or {}).get("title") or "")
    artist = _clean_artist((meta or {}).get("artist") or "")
    album = _clean_title((meta or {}).get("album") or "")
    source_title = title or os.path.splitext(os.path.basename(file_path))[0]

    if not artist and " - " in source_title:
        parts = source_title.split(" - ", 1)
        artist = _clean_artist(parts[0].strip())
        title = _clean_title(parts[1].strip()) if len(parts) > 1 else title
    if not title:
        title = _clean_title(source_title)

    return {
        "artist": artist.strip() if artist else "",
        "title": title.strip() if title else "",
        "album": album.strip() if album else "",
        "source_title": source_title,
    }


def get_duration_seconds(file_path):
    try:
        audio = MutagenFile(file_path)
        if audio and audio.info and audio.info.length:
            return int(round(audio.info.length))
    except Exception:
        return None
    return None


def merge_candidates(existing, extra):
    by_id = {}
    for item in existing or []:
        key = item.get("recording_id") or id(item)
        by_id[key] = item
    for item in extra or []:
        key = item.get("recording_id") or id(item)
        if key not in by_id:
            by_id[key] = item
    return list(by_id.values())


def select_best_match(source, candidates, duration):
    best = None
    best_score = 0
    for candidate in candidates or []:
        score = score_match(source, candidate, duration)
        if score > best_score:
            best = candidate
            best_score = score
    return best, best_score


def score_match(source, candidate, duration):
    score = 0
    artist_score = _fuzzy_score(source.get("artist"), candidate.get("artist"))
    if artist_score >= 80:
        score += 40
    title_score = _fuzzy_score(source.get("title"), candidate.get("title"))
    if title_score >= 80:
        score += 30
    album_score = _fuzzy_score(source.get("album"), candidate.get("album"))
    if source.get("album") and album_score >= 80:
        score += 10
    if duration and candidate.get("duration"):
        try:
            cand_duration = int(round(candidate["duration"]))
        except Exception:
            cand_duration = None
        if cand_duration is not None and abs(cand_duration - duration) <= 2:
            score += 20
    return score


def _fuzzy_score(left, right):
    if not left or not right:
        return 0
    return int(fuzz.token_set_ratio(left, right))


def _clean_title(value):
    if not value:
        return ""
    cleaned = _TITLE_CLEAN_RE.sub(" ", value)
    cleaned = _TITLE_TRAIL_RE.sub("", cleaned)
    return " ".join(cleaned.split())


def _clean_artist(value):
    if not value:
        return ""
    cleaned = value.strip()
    if cleaned.startswith("@"):
        cleaned = cleaned.lstrip("@").strip()
    cleaned = _VEVO_SUFFIX_RE.sub("", cleaned).strip()
    return cleaned
