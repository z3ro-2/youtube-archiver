import re
import unicodedata
from dataclasses import dataclass

_WEIGHT_ARTIST = 0.30
_WEIGHT_TRACK = 0.35
_WEIGHT_ALBUM = 0.15
_WEIGHT_DURATION = 0.15
_WEIGHT_BONUS = 0.05
_BASELINE_NEUTRAL = 0.60

_FEAT_RE = re.compile(r"\b(featuring|feat\.?|ft\.?)\b")
_BRACKET_RE = re.compile(r"[\(\[\{][^)\]\}]*[\)\]\}]")
_PUNCT_RE = re.compile(r"[^\w\s/&]+")
_WHITESPACE_RE = re.compile(r"\s+")

_PENALTY_TERMS = {"cover", "tribute", "karaoke", "reaction", "8d", "nightcore", "slowed"}
_LIVE_TERMS = {"live"}
_REMASTER_TERMS = {"remaster", "remastered"}


@dataclass(frozen=True)
class ScoreBreakdown:
    score_artist: float
    score_track: float
    score_album: float
    score_duration: float
    bonus_score: float
    weighted_sum: float
    source_modifier: float
    penalty_multiplier: float
    final_score: float


def normalize_text(value):
    if not value:
        return ""
    text = unicodedata.normalize("NFKD", value)
    text = text.lower().strip()
    text = _FEAT_RE.sub("feat", text)
    text = _BRACKET_RE.sub(" ", text)
    text = text.replace("_", " ")
    text = _PUNCT_RE.sub(" ", text)
    text = _WHITESPACE_RE.sub(" ", text).strip()
    return text


def tokenize(value):
    normalized = normalize_text(value)
    if not normalized:
        return []
    return normalized.split()


def clamp01(value):
    if value < 0:
        return 0.0
    if value > 1:
        return 1.0
    return float(value)


def token_similarity(target_tokens, candidate_tokens):
    if not target_tokens or not candidate_tokens:
        return 0.0
    target_set = set(target_tokens)
    candidate_set = set(candidate_tokens)
    common = len(target_set & candidate_set)
    return common / max(len(target_set), len(candidate_set))


def duration_score(target_sec, candidate_sec):
    if target_sec is None or candidate_sec is None:
        return _BASELINE_NEUTRAL
    delta = abs(int(target_sec) - int(candidate_sec))
    if delta <= 2:
        return 1.0
    if delta <= 5:
        return 0.90
    if delta <= 10:
        return 0.75
    if delta <= 20:
        return 0.50
    return 0.20


def _has_terms(tokens, terms):
    return bool(set(tokens) & set(terms))


def _penalty_multiplier(target_track_tokens, candidate_tokens, artist_score):
    multiplier = 1.0
    if _has_terms(candidate_tokens, _PENALTY_TERMS) and not _has_terms(target_track_tokens, _PENALTY_TERMS):
        multiplier *= 0.10
    if _has_terms(candidate_tokens, _LIVE_TERMS) != _has_terms(target_track_tokens, _LIVE_TERMS):
        multiplier *= 0.85
    if _has_terms(candidate_tokens, _REMASTER_TERMS) != _has_terms(target_track_tokens, _REMASTER_TERMS):
        multiplier *= 0.92
    if artist_score < 0.50:
        multiplier *= 0.50
    return multiplier


def score_candidate(target, candidate, *, source_modifier):
    target_artist = target.get("artist") or ""
    target_track = target.get("track") or ""
    target_album = target.get("album") or ""
    target_duration = target.get("duration_hint_sec")

    candidate_artist = candidate.get("artist") or candidate.get("uploader") or ""
    candidate_track = candidate.get("track") or candidate.get("title") or ""
    candidate_album = candidate.get("album") or ""
    candidate_duration = candidate.get("duration_sec")
    candidate_title_tokens = tokenize(candidate.get("title") or "")

    target_artist_tokens = tokenize(target_artist)
    target_track_tokens = tokenize(target_track)
    target_album_tokens = tokenize(target_album)

    candidate_artist_tokens = tokenize(candidate_artist)
    candidate_track_tokens = tokenize(candidate_track)
    candidate_album_tokens = tokenize(candidate_album)

    score_artist = token_similarity(target_artist_tokens, candidate_artist_tokens)
    if target_track_tokens:
        score_track = token_similarity(target_track_tokens, candidate_track_tokens)
    else:
        score_track = _BASELINE_NEUTRAL
    if target_album_tokens and candidate_album_tokens:
        score_album = token_similarity(target_album_tokens, candidate_album_tokens)
    else:
        score_album = _BASELINE_NEUTRAL
    score_duration = duration_score(target_duration, candidate_duration)
    bonus_score = 0.0

    weighted_sum = (
        _WEIGHT_ARTIST * score_artist
        + _WEIGHT_TRACK * score_track
        + _WEIGHT_ALBUM * score_album
        + _WEIGHT_DURATION * score_duration
        + _WEIGHT_BONUS * bonus_score
    )
    weighted_sum = clamp01(weighted_sum)
    penalty_tokens = list(set(candidate_track_tokens) | set(candidate_title_tokens))
    penalty_multiplier = _penalty_multiplier(target_track_tokens, penalty_tokens, score_artist)
    final_score = weighted_sum * source_modifier * penalty_multiplier

    return ScoreBreakdown(
        score_artist=score_artist,
        score_track=score_track,
        score_album=score_album,
        score_duration=score_duration,
        bonus_score=bonus_score,
        weighted_sum=weighted_sum,
        source_modifier=source_modifier,
        penalty_multiplier=penalty_multiplier,
        final_score=final_score,
    )


def rank_candidates(target, candidates, *, source_priority):
    scored = []
    source_rank = {name: idx for idx, name in enumerate(source_priority)}
    for candidate in candidates:
        modifier = candidate.get("source_modifier", 1.0)
        breakdown = score_candidate(target, candidate, source_modifier=modifier)
        scored.append((candidate, breakdown))
    scored.sort(
        key=lambda item: (
            -item[1].final_score,
            source_rank.get(item[0].get("source"), 999),
            item[0].get("url") or "",
        )
    )
    ranked = []
    for idx, (candidate, breakdown) in enumerate(scored, start=1):
        ranked.append((candidate, breakdown, idx))
    return ranked
