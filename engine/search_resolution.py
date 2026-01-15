import json
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime
from uuid import uuid4

from engine.job_queue import DownloadJobStore
from engine.paths import DOWNLOADS_DIR, resolve_dir
from engine.search_adapters import BandcampAdapter, SoundCloudAdapter, YouTubeMusicAdapter
from engine.search_scoring import rank_candidates

_REQUEST_STATUSES = {"queued", "resolving", "ready", "running", "completed", "failed", "canceled"}
_ITEM_STATUSES = {"queued", "searching", "candidate_found", "selected", "enqueued", "skipped", "failed"}
_INTENTS = {"track", "album", "artist", "artist_collection"}
_MEDIA_TYPES = {"audio", "video"}
_DEFAULT_SOURCE_PRIORITY = ["bandcamp", "youtube_music", "soundcloud"]


def _utc_now():
    return datetime.utcnow().isoformat()


def _log_event(level, payload):
    message = json.dumps(payload, sort_keys=True, default=str)
    getattr(logging, level)(message)


def _as_bool(value, default):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _parse_source_priority(value):
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return list(_DEFAULT_SOURCE_PRIORITY)
        if isinstance(parsed, list) and all(isinstance(item, str) for item in parsed):
            return parsed
    return list(_DEFAULT_SOURCE_PRIORITY)


def _serialize_source_priority(value):
    return json.dumps(_parse_source_priority(value))


def ensure_search_db(conn):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS search_requests (
            id TEXT PRIMARY KEY,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            created_by TEXT,
            intent TEXT NOT NULL,
            media_type TEXT NOT NULL,
            artist TEXT NOT NULL,
            album TEXT,
            track TEXT,
            include_albums INTEGER DEFAULT 1,
            include_singles INTEGER DEFAULT 1,
            min_match_score REAL DEFAULT 0.92,
            duration_hint_sec INTEGER,
            quality_min_bitrate_kbps INTEGER,
            lossless_only INTEGER DEFAULT 0,
            source_priority_json TEXT NOT NULL,
            max_candidates_per_source INTEGER DEFAULT 5,
            status TEXT NOT NULL,
            error TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS search_items (
            id TEXT PRIMARY KEY,
            request_id TEXT NOT NULL,
            position INTEGER NOT NULL,
            item_type TEXT NOT NULL,
            media_type TEXT NOT NULL,
            artist TEXT NOT NULL,
            album TEXT,
            track TEXT,
            duration_hint_sec INTEGER,
            status TEXT NOT NULL,
            chosen_source TEXT,
            chosen_url TEXT,
            chosen_score REAL,
            error TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS search_candidates (
            id TEXT PRIMARY KEY,
            item_id TEXT NOT NULL,
            source TEXT NOT NULL,
            url TEXT NOT NULL,
            title TEXT NOT NULL,
            uploader TEXT,
            artist_detected TEXT,
            album_detected TEXT,
            track_detected TEXT,
            duration_sec INTEGER,
            artwork_url TEXT,
            raw_meta_json TEXT,
            score_artist REAL,
            score_track REAL,
            score_album REAL,
            score_duration REAL,
            source_modifier REAL,
            penalty_multiplier REAL,
            final_score REAL,
            rank INTEGER
        )
        """
    )
    conn.commit()

    _ensure_columns(
        conn,
        "search_requests",
        {
            "created_by": "created_by TEXT",
            "intent": "intent TEXT",
            "media_type": "media_type TEXT",
            "artist": "artist TEXT",
            "album": "album TEXT",
            "track": "track TEXT",
            "include_albums": "include_albums INTEGER DEFAULT 1",
            "include_singles": "include_singles INTEGER DEFAULT 1",
            "min_match_score": "min_match_score REAL DEFAULT 0.92",
            "duration_hint_sec": "duration_hint_sec INTEGER",
            "quality_min_bitrate_kbps": "quality_min_bitrate_kbps INTEGER",
            "lossless_only": "lossless_only INTEGER DEFAULT 0",
            "source_priority_json": "source_priority_json TEXT",
            "max_candidates_per_source": "max_candidates_per_source INTEGER DEFAULT 5",
            "status": "status TEXT",
            "error": "error TEXT",
        },
    )
    _ensure_columns(
        conn,
        "search_items",
        {
            "request_id": "request_id TEXT",
            "position": "position INTEGER",
            "item_type": "item_type TEXT",
            "media_type": "media_type TEXT",
            "artist": "artist TEXT",
            "album": "album TEXT",
            "track": "track TEXT",
            "duration_hint_sec": "duration_hint_sec INTEGER",
            "status": "status TEXT",
            "chosen_source": "chosen_source TEXT",
            "chosen_url": "chosen_url TEXT",
            "chosen_score": "chosen_score REAL",
            "error": "error TEXT",
        },
    )
    _ensure_columns(
        conn,
        "search_candidates",
        {
            "item_id": "item_id TEXT",
            "source": "source TEXT",
            "url": "url TEXT",
            "title": "title TEXT",
            "uploader": "uploader TEXT",
            "artist_detected": "artist_detected TEXT",
            "album_detected": "album_detected TEXT",
            "track_detected": "track_detected TEXT",
            "duration_sec": "duration_sec INTEGER",
            "artwork_url": "artwork_url TEXT",
            "raw_meta_json": "raw_meta_json TEXT",
            "score_artist": "score_artist REAL",
            "score_track": "score_track REAL",
            "score_album": "score_album REAL",
            "score_duration": "score_duration REAL",
            "source_modifier": "source_modifier REAL",
            "penalty_multiplier": "penalty_multiplier REAL",
            "final_score": "final_score REAL",
            "rank": "rank INTEGER",
        },
    )

    cur.execute("CREATE INDEX IF NOT EXISTS idx_search_requests_status ON search_requests (status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_search_requests_created_at ON search_requests (created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_search_items_request_status ON search_items (request_id, status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_search_items_status ON search_items (status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_search_candidates_item_score ON search_candidates (item_id, final_score DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_search_candidates_source ON search_candidates (source)")
    conn.commit()


def _ensure_columns(conn, table, columns):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in cur.fetchall()}
    for name, ddl in columns.items():
        if name not in existing:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")
            _log_event("warning", {"event": "search_db_migration", "table": table, "column": name})
    conn.commit()


class SearchResolutionService:
    def __init__(self, *, search_db_path, download_db_path, config=None, adapters=None):
        self.search_db_path = search_db_path
        self.download_db_path = download_db_path
        self.config = config or {}
        self.adapters = adapters or {
            "bandcamp": BandcampAdapter(),
            "youtube_music": YouTubeMusicAdapter(),
            "soundcloud": SoundCloudAdapter(),
        }
        self._lock = threading.Lock()

    def _connect(self):
        os.makedirs(os.path.dirname(self.search_db_path) or ".", exist_ok=True)
        conn = sqlite3.connect(self.search_db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        ensure_search_db(conn)
        return conn

    def create_search_request(self, payload):
        intent = (payload.get("intent") or "").strip().lower()
        media_type = (payload.get("media_type") or "audio").strip().lower()
        artist = (payload.get("artist") or "").strip()
        album = (payload.get("album") or "").strip() or None
        track = (payload.get("track") or "").strip() or None
        if intent not in _INTENTS:
            raise ValueError("intent must be track, album, artist, or artist_collection")
        if media_type not in _MEDIA_TYPES:
            raise ValueError("media_type must be audio or video")
        if not artist:
            raise ValueError("artist is required")
        if intent == "track" and not track:
            raise ValueError("track is required for track intent")
        if intent == "album" and not album:
            raise ValueError("album is required for album intent")
        include_albums = _as_bool(payload.get("include_albums"), True)
        include_singles = _as_bool(payload.get("include_singles"), True)
        min_match_score = payload.get("min_match_score")
        if isinstance(min_match_score, str):
            try:
                min_match_score = float(min_match_score)
            except ValueError:
                min_match_score = None
        if not isinstance(min_match_score, (int, float)):
            min_match_score = 0.92
        max_candidates = payload.get("max_candidates_per_source")
        if isinstance(max_candidates, str) and max_candidates.isdigit():
            max_candidates = int(max_candidates)
        if not isinstance(max_candidates, int) or max_candidates <= 0:
            max_candidates = 5
        source_priority_json = _serialize_source_priority(payload.get("source_priority_json"))
        duration_hint = payload.get("duration_hint_sec")
        if isinstance(duration_hint, str) and duration_hint.isdigit():
            duration_hint = int(duration_hint)
        if not isinstance(duration_hint, int):
            duration_hint = None
        quality_min = payload.get("quality_min_bitrate_kbps")
        if isinstance(quality_min, str) and quality_min.isdigit():
            quality_min = int(quality_min)
        if not isinstance(quality_min, int):
            quality_min = None
        lossless_only = _as_bool(payload.get("lossless_only"), False)
        created_by = payload.get("created_by") or ""

        now = _utc_now()
        request_id = uuid4().hex
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO search_requests (
                    id, created_at, updated_at, created_by, intent, media_type, artist,
                    album, track, include_albums, include_singles, min_match_score,
                    duration_hint_sec, quality_min_bitrate_kbps, lossless_only,
                    source_priority_json, max_candidates_per_source, status, error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    request_id,
                    now,
                    now,
                    created_by,
                    intent,
                    media_type,
                    artist,
                    album,
                    track,
                    1 if include_albums else 0,
                    1 if include_singles else 0,
                    float(min_match_score),
                    duration_hint,
                    quality_min,
                    1 if lossless_only else 0,
                    source_priority_json,
                    max_candidates,
                    "queued",
                    None,
                ),
            )
        _log_event(
            "info",
            {
                "event": "search_request_created",
                "request_id": request_id,
                "status": "queued",
                "intent": intent,
                "media_type": media_type,
            },
        )
        return request_id

    def get_search_request(self, request_id):
        with self._connect() as conn:
            req = conn.execute("SELECT * FROM search_requests WHERE id=?", (request_id,)).fetchone()
            if not req:
                return None
            summary_rows = conn.execute(
                "SELECT status, COUNT(*) AS count FROM search_items WHERE request_id=? GROUP BY status",
                (request_id,),
            ).fetchall()
            summary = {row["status"]: row["count"] for row in summary_rows}
            return {
                **dict(req),
                "summary": summary,
            }

    def list_search_requests(self, status=None, limit=50):
        if status and status not in _REQUEST_STATUSES:
            raise ValueError("invalid status")
        limit = int(limit) if isinstance(limit, int) or (isinstance(limit, str) and limit.isdigit()) else 50
        limit = max(1, min(limit, 200))
        with self._connect() as conn:
            if status:
                rows = conn.execute(
                    "SELECT * FROM search_requests WHERE status=? ORDER BY created_at ASC LIMIT ?",
                    (status, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM search_requests ORDER BY created_at ASC LIMIT ?",
                    (limit,),
                ).fetchall()
        return [dict(row) for row in rows]

    def list_search_items(self, request_id):
        return self._list_items(request_id)

    def list_search_candidates(self, item_id):
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    id,
                    item_id,
                    source,
                    url,
                    title,
                    uploader,
                    artist_detected,
                    album_detected,
                    track_detected,
                    duration_sec,
                    artwork_url,
                    score_artist,
                    score_track,
                    score_album,
                    score_duration,
                    source_modifier,
                    penalty_multiplier,
                    final_score,
                    rank
                FROM search_candidates
                WHERE item_id=?
                ORDER BY rank ASC
                """,
                (item_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def cancel_search_request(self, request_id):
        now = _utc_now()
        with self._connect() as conn:
            items = conn.execute(
                """
                SELECT id FROM search_items
                WHERE request_id=? AND status IN ('queued', 'searching', 'candidate_found', 'selected')
                """,
                (request_id,),
            ).fetchall()
            cur = conn.execute(
                """
                UPDATE search_requests
                SET status='canceled', updated_at=?, error=?
                WHERE id=? AND status NOT IN ('completed', 'failed', 'canceled')
                """,
                (now, "canceled", request_id),
            )
            if cur.rowcount == 0:
                return False
            conn.execute(
                """
                UPDATE search_items
                SET status='skipped', error=?
                WHERE request_id=? AND status IN ('queued', 'searching', 'candidate_found', 'selected')
                """,
                ("request_canceled", request_id),
            )
        for row in items:
            _log_event(
                "info",
                {
                    "event": "search_item_status",
                    "request_id": request_id,
                    "item_id": row["id"],
                    "status": "skipped",
                    "error": "request_canceled",
                },
            )
        _log_event(
            "info",
            {"event": "search_request_canceled", "request_id": request_id, "status": "canceled"},
        )
        return True

    def run_search_resolution_once(self, *, config=None):
        with self._lock:
            config = config or self.config or {}
            request = self._claim_next_request()
            if not request:
                return None
            request_id = request["id"]

            if request["intent"] in {"artist", "artist_collection"}:
                self._update_request_status(request_id, "failed", "not_implemented")
                return request_id

            self._ensure_items(request)
            self._update_request_status(request_id, "running", None)
            items = self._list_items(request_id)
            for item in items:
                if item["status"] not in {"queued", "searching", "candidate_found"}:
                    continue
                self._process_item(request, item, config)
            self._finalize_request(request_id)
            return request_id

    def run_search_resolution_loop(self, *, stop_event, poll_interval_seconds=2):
        while not (stop_event and stop_event.is_set()):
            request_id = self.run_search_resolution_once()
            if not request_id:
                time.sleep(poll_interval_seconds)

    def _claim_next_request(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            row = cur.execute(
                """
                SELECT * FROM search_requests
                WHERE status='queued'
                ORDER BY created_at ASC
                LIMIT 1
                """
            ).fetchone()
            if not row:
                conn.commit()
                return None
            request_id = row["id"]
            cur.execute(
                "UPDATE search_requests SET status='resolving', updated_at=? WHERE id=? AND status='queued'",
                (_utc_now(), request_id),
            )
            if cur.rowcount != 1:
                conn.commit()
                return None
            conn.commit()
            _log_event(
                "info",
                {"event": "search_request_status", "request_id": request_id, "status": "resolving"},
            )
            return dict(row)

    def _update_request_status(self, request_id, status, error):
        now = _utc_now()
        with self._connect() as conn:
            conn.execute(
                "UPDATE search_requests SET status=?, updated_at=?, error=? WHERE id=?",
                (status, now, error, request_id),
            )
        _log_event(
            "info",
            {"event": "search_request_status", "request_id": request_id, "status": status, "error": error},
        )

    def _ensure_items(self, request):
        request_id = request["id"]
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT 1 FROM search_items WHERE request_id=? LIMIT 1",
                (request_id,),
            ).fetchone()
            if existing:
                return
            now = _utc_now()
            items = []
            intent = request["intent"]
            if intent == "track":
                items.append({
                    "id": uuid4().hex,
                    "request_id": request_id,
                    "position": 1,
                    "item_type": "track",
                    "media_type": request["media_type"],
                    "artist": request["artist"],
                    "album": request["album"],
                    "track": request["track"],
                    "duration_hint_sec": request["duration_hint_sec"],
                    "status": "queued",
                })
            elif intent == "album":
                items.append({
                    "id": uuid4().hex,
                    "request_id": request_id,
                    "position": 1,
                    "item_type": "album",
                    "media_type": request["media_type"],
                    "artist": request["artist"],
                    "album": request["album"],
                    "track": None,
                    "duration_hint_sec": request["duration_hint_sec"],
                    "status": "queued",
                })
            for item in items:
                conn.execute(
                    """
                    INSERT INTO search_items (
                        id, request_id, position, item_type, media_type, artist, album,
                        track, duration_hint_sec, status, chosen_source, chosen_url, chosen_score, error
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        item["id"],
                        item["request_id"],
                        item["position"],
                        item["item_type"],
                        item["media_type"],
                        item["artist"],
                        item["album"],
                        item["track"],
                        item["duration_hint_sec"],
                        item["status"],
                        None,
                        None,
                        None,
                        None,
                    ),
                )
        _log_event(
            "info",
            {"event": "search_items_created", "request_id": request_id, "count": len(items)},
        )

    def _list_items(self, request_id):
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM search_items WHERE request_id=? ORDER BY position ASC",
                (request_id,),
            ).fetchall()
            return [dict(row) for row in rows]

    def _process_item(self, request, item, config):
        request_id = request["id"]
        item_id = item["id"]
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE search_items SET status='searching' WHERE id=? AND status='queued'",
                (item_id,),
            )
            if cur.rowcount != 1:
                return
        _log_event(
            "info",
            {
                "event": "search_item_status",
                "request_id": request_id,
                "item_id": item_id,
                "status": "searching",
            },
        )

        source_priority = _parse_source_priority(request.get("source_priority_json"))
        max_candidates = request.get("max_candidates_per_source") or 5
        target = {
            "artist": item.get("artist"),
            "track": item.get("track"),
            "album": item.get("album"),
            "duration_hint_sec": item.get("duration_hint_sec"),
        }
        candidates = []
        for source in source_priority:
            adapter = self.adapters.get(source)
            if not adapter:
                continue
            if item["item_type"] == "track":
                results = adapter.search_track(
                    item.get("artist"),
                    item.get("track"),
                    item.get("album"),
                    limit=max_candidates,
                ) or []
            else:
                results = adapter.search_album(
                    item.get("artist"),
                    item.get("album"),
                    limit=max_candidates,
                ) or []
            for result in results[:max_candidates]:
                candidate = {
                    "source": source,
                    "url": result.get("url"),
                    "title": result.get("title") or "",
                    "uploader": result.get("uploader"),
                    "artist": result.get("artist"),
                    "album": result.get("album"),
                    "track": result.get("track"),
                    "duration_sec": result.get("duration_sec"),
                    "artwork_url": result.get("artwork_url"),
                    "raw_meta": result.get("raw_meta") or {},
                    "source_modifier": adapter.source_modifier(result),
                    "is_official": result.get("is_official"),
                }
                if candidate["url"]:
                    candidates.append(candidate)

        if not candidates:
            self._update_item_status(item_id, "failed", "no_candidates", request_id=request_id)
            return

        ranked = rank_candidates(target, candidates, source_priority=source_priority)
        with self._connect() as conn:
            for candidate, breakdown, rank in ranked:
                conn.execute(
                    """
                    INSERT INTO search_candidates (
                        id, item_id, source, url, title, uploader, artist_detected,
                        album_detected, track_detected, duration_sec, artwork_url, raw_meta_json,
                        score_artist, score_track, score_album, score_duration, source_modifier,
                        penalty_multiplier, final_score, rank
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        uuid4().hex,
                        item_id,
                        candidate["source"],
                        candidate["url"],
                        candidate["title"],
                        candidate.get("uploader"),
                        candidate.get("artist"),
                        candidate.get("album"),
                        candidate.get("track"),
                        candidate.get("duration_sec"),
                        candidate.get("artwork_url"),
                        json.dumps(candidate.get("raw_meta") or {}, sort_keys=True, default=str),
                        breakdown.score_artist,
                        breakdown.score_track,
                        breakdown.score_album,
                        breakdown.score_duration,
                        breakdown.source_modifier,
                        breakdown.penalty_multiplier,
                        breakdown.final_score,
                        rank,
                    ),
                )

        self._update_item_status(item_id, "candidate_found", None, request_id=request_id)

        min_match_score = request.get("min_match_score")
        if not isinstance(min_match_score, (int, float)):
            min_match_score = 0.92
        chosen = None
        chosen_breakdown = None
        for candidate, breakdown, _rank in ranked:
            if breakdown.final_score >= min_match_score:
                chosen = candidate
                chosen_breakdown = breakdown
                break

        if not chosen:
            self._update_item_status(item_id, "failed", "no_candidate_above_threshold", request_id=request_id)
            return

        self._update_item_choice(
            item_id,
            source=chosen["source"],
            url=chosen["url"],
            score=chosen_breakdown.final_score,
            status="selected",
            request_id=request_id,
        )
        enqueued, trace_id = self._enqueue_download_job(request, item, chosen, config, chosen_breakdown)
        if enqueued:
            self._update_item_status(
                item_id,
                "enqueued",
                None,
                request_id=request_id,
                source=chosen["source"],
                trace_id=trace_id,
            )
        else:
            self._update_item_status(item_id, "failed", "enqueue_failed", request_id=request_id, source=chosen["source"])

    def _update_item_status(self, item_id, status, error, *, request_id=None, source=None, trace_id=None):
        if status not in _ITEM_STATUSES:
            raise ValueError("invalid status")
        with self._connect() as conn:
            conn.execute(
                "UPDATE search_items SET status=?, error=? WHERE id=?",
                (status, error, item_id),
            )
        payload = {
            "event": "search_item_status",
            "item_id": item_id,
            "status": status,
            "error": error,
        }
        if request_id:
            payload["request_id"] = request_id
        if source:
            payload["source"] = source
        if trace_id:
            payload["trace_id"] = trace_id
        _log_event("info", payload)

    def _update_item_choice(self, item_id, *, source, url, score, status, request_id=None):
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE search_items
                SET chosen_source=?, chosen_url=?, chosen_score=?, status=?
                WHERE id=?
                """,
                (source, url, score, status, item_id),
            )
        _log_event(
            "info",
            {
                "event": "search_item_selected",
                "request_id": request_id,
                "item_id": item_id,
                "status": status,
                "source": source,
                "url": url,
                "score": score,
            },
        )

    def _resolve_output_dir(self, config):
        base_dir = DOWNLOADS_DIR
        configured = None
        if isinstance(config, dict):
            configured = config.get("single_download_folder")
        try:
            return resolve_dir(configured, base_dir)
        except ValueError:
            return base_dir

    def _resolve_output_template(self, media_type, config):
        if not isinstance(config, dict):
            return None
        if media_type == "audio":
            return config.get("music_filename_template")
        return config.get("filename_template")

    def _enqueue_download_job(self, request, item, candidate, config, breakdown):
        output_dir = self._resolve_output_dir(config)
        output_template = self._resolve_output_template(item["media_type"], config)
        store = DownloadJobStore(self.download_db_path)
        if store.has_job_for_origin("search", request["id"], candidate["url"]):
            _log_event(
                "info",
                {
                    "event": "download_job_exists",
                    "request_id": request["id"],
                    "item_id": item["id"],
                    "source": candidate["source"],
                    "url": candidate["url"],
                },
            )
            return True, None

        trace_id = uuid4().hex
        metadata = {
            "title": candidate.get("title"),
            "artist": candidate.get("artist"),
            "album": candidate.get("album"),
            "track": candidate.get("track") or candidate.get("title"),
            "url": candidate.get("url"),
            "duration_sec": candidate.get("duration_sec"),
        }
        store.enqueue(
            origin="search",
            origin_id=request["id"],
            media_type=item["media_type"],
            media_intent=item["item_type"],
            source=candidate["source"],
            url=candidate["url"],
            output_template=output_template,
            output_dir=output_dir,
            context={
                "request_id": request["id"],
                "item_id": item["id"],
                "target_format": config.get("final_format") if isinstance(config, dict) else None,
                "audio_only": item["media_type"] == "audio",
                "metadata": metadata,
                "source_modifier": breakdown.source_modifier,
                "final_score": breakdown.final_score,
                "trace_id": trace_id,
            },
            max_attempts=config.get("job_max_attempts") if isinstance(config, dict) else None,
            trace_id=trace_id,
        )
        _log_event(
            "info",
            {
                "event": "download_job_enqueued",
                "request_id": request["id"],
                "item_id": item["id"],
                "trace_id": trace_id,
                "source": candidate["source"],
                "url": candidate["url"],
            },
        )
        return True, trace_id

    def _finalize_request(self, request_id):
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT status FROM search_items WHERE request_id=?",
                (request_id,),
            ).fetchall()
        statuses = [row["status"] for row in rows]
        if any(status in {"queued", "searching", "candidate_found", "selected"} for status in statuses):
            self._update_request_status(request_id, "running", None)
            return
        if any(status == "enqueued" for status in statuses):
            self._update_request_status(request_id, "completed", None)
            return
        self._update_request_status(request_id, "failed", "no_items_enqueued")
