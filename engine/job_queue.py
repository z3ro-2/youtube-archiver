import json
import logging
import os
import shutil
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import uuid4

_ALLOWED_ORIGINS = {"playlist", "search"}
_ALLOWED_MEDIA_TYPES = {"audio", "video"}
_ALLOWED_MEDIA_INTENTS = {"track", "album", "playlist", "episode", "movie"}
_TERMINAL_STATUSES = {"completed", "failed", "canceled"}
_DEFAULT_MAX_ATTEMPTS = 3
_DEFAULT_RETRY_DELAY_SECONDS = 30
_DEFAULT_POLL_INTERVAL_SECONDS = 1.0


def _utc_now():
    return datetime.utcnow().isoformat()


def _parse_context(raw):
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


def _serialize_context(context):
    if not context:
        return None
    return json.dumps(context, sort_keys=True, default=str)


def _job_log(level, *, trace_id, job_id, source, event, **fields):
    payload = {
        "event": event,
        "trace_id": trace_id,
        "job_id": job_id,
        "source": source,
        **fields,
    }
    message = json.dumps(payload, sort_keys=True, default=str)
    getattr(logging, level)(message)


def ensure_download_jobs_table(conn):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS download_jobs (
            id TEXT PRIMARY KEY,
            origin TEXT NOT NULL,
            origin_id TEXT NOT NULL,
            media_type TEXT NOT NULL,
            media_intent TEXT NOT NULL,
            source TEXT NOT NULL,
            url TEXT NOT NULL,
            output_template TEXT,
            output_dir TEXT NOT NULL,
            status TEXT NOT NULL,
            queued TIMESTAMP,
            running TIMESTAMP,
            completed TIMESTAMP,
            failed TIMESTAMP,
            canceled TIMESTAMP,
            attempts INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT 3,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            last_error TEXT,
            trace_id TEXT NOT NULL UNIQUE,
            context_json TEXT
        )
        """
    )
    existing = {row[1] for row in cur.execute("PRAGMA table_info(download_jobs)").fetchall()}
    missing = []
    columns = {
        "output_template": "output_template TEXT",
        "output_dir": "output_dir TEXT",
        "origin": "origin TEXT",
        "origin_id": "origin_id TEXT",
        "media_type": "media_type TEXT",
        "media_intent": "media_intent TEXT",
        "source": "source TEXT",
        "url": "url TEXT",
        "status": "status TEXT",
        "queued": "queued TIMESTAMP",
        "running": "running TIMESTAMP",
        "completed": "completed TIMESTAMP",
        "failed": "failed TIMESTAMP",
        "canceled": "canceled TIMESTAMP",
        "attempts": "attempts INTEGER DEFAULT 0",
        "max_attempts": "max_attempts INTEGER DEFAULT 3",
        "created_at": "created_at TIMESTAMP",
        "updated_at": "updated_at TIMESTAMP",
        "last_error": "last_error TEXT",
        "trace_id": "trace_id TEXT",
        "context_json": "context_json TEXT",
    }
    for name, ddl in columns.items():
        if name not in existing:
            missing.append((name, ddl))
    for name, ddl in missing:
        cur.execute(f"ALTER TABLE download_jobs ADD COLUMN {ddl}")
        logging.warning("Migrated download_jobs: added column %s", name)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_download_jobs_status ON download_jobs (status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_download_jobs_source_status ON download_jobs (source, status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_download_jobs_created_at ON download_jobs (created_at)")

    cur.execute(
        """
        CREATE TRIGGER IF NOT EXISTS download_jobs_immutable_fields
        BEFORE UPDATE ON download_jobs
        FOR EACH ROW
        WHEN
            OLD.source != NEW.source
            OR OLD.url != NEW.url
            OR COALESCE(OLD.output_template, '') != COALESCE(NEW.output_template, '')
            OR OLD.media_intent != NEW.media_intent
        BEGIN
            SELECT RAISE(ABORT, 'download_jobs immutable field update blocked');
        END
        """
    )
    conn.commit()


@dataclass(frozen=True)
class DownloadJob:
    id: str
    origin: str
    origin_id: str
    media_type: str
    media_intent: str
    source: str
    url: str
    output_template: str | None
    output_dir: str
    status: str
    queued: str | None
    running: str | None
    completed: str | None
    failed: str | None
    canceled: str | None
    attempts: int
    max_attempts: int
    created_at: str
    updated_at: str
    last_error: str | None
    trace_id: str
    context: dict

    @classmethod
    def from_row(cls, row):
        return cls(
            id=row["id"],
            origin=row["origin"],
            origin_id=row["origin_id"],
            media_type=row["media_type"],
            media_intent=row["media_intent"],
            source=row["source"],
            url=row["url"],
            output_template=row["output_template"],
            output_dir=row["output_dir"],
            status=row["status"],
            queued=row["queued"],
            running=row["running"],
            completed=row["completed"],
            failed=row["failed"],
            canceled=row["canceled"],
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            last_error=row["last_error"],
            trace_id=row["trace_id"],
            context=_parse_context(row["context_json"]),
        )


class DownloadJobStore:
    def __init__(self, db_path):
        self.db_path = db_path

    def _connect(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    def enqueue(
        self,
        *,
        origin,
        origin_id,
        media_type,
        media_intent,
        source,
        url,
        output_template,
        output_dir,
        context=None,
        max_attempts=None,
        trace_id=None,
        job_id=None,
    ):
        if origin not in _ALLOWED_ORIGINS:
            raise ValueError(f"Invalid origin: {origin}")
        if media_type not in _ALLOWED_MEDIA_TYPES:
            raise ValueError(f"Invalid media_type: {media_type}")
        if media_intent not in _ALLOWED_MEDIA_INTENTS:
            raise ValueError(f"Invalid media_intent: {media_intent}")
        if not source:
            raise ValueError("source is required")
        if not url:
            raise ValueError("url is required")
        if not output_dir:
            raise ValueError("output_dir is required")
        now = _utc_now()
        job_id = job_id or uuid4().hex
        trace_id = trace_id or uuid4().hex
        max_attempts = max_attempts or _DEFAULT_MAX_ATTEMPTS
        payload = _serialize_context(context)
        with self._connect() as conn:
            ensure_download_jobs_table(conn)
            conn.execute(
                """
                INSERT INTO download_jobs (
                    id, origin, origin_id, media_type, media_intent, source, url,
                    output_template, output_dir, status, queued, attempts, max_attempts,
                    created_at, updated_at, trace_id, context_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    origin,
                    origin_id,
                    media_type,
                    media_intent,
                    source,
                    url,
                    output_template,
                    output_dir,
                    "queued",
                    now,
                    0,
                    int(max_attempts),
                    now,
                    now,
                    trace_id,
                    payload,
                ),
            )
        _job_log(
            "info",
            trace_id=trace_id,
            job_id=job_id,
            source=source,
            event="job_enqueued",
            status="queued",
            origin=origin,
            media_type=media_type,
            media_intent=media_intent,
        )
        return job_id

    def claim_next(self, source, *, now=None):
        now = now or _utc_now()
        with self._connect() as conn:
            ensure_download_jobs_table(conn)
            cur = conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            cur.execute(
                """
                SELECT * FROM download_jobs
                WHERE status='queued' AND source=? AND (queued IS NULL OR queued <= ?)
                ORDER BY queued ASC, created_at ASC
                LIMIT 1
                """,
                (source, now),
            )
            row = cur.fetchone()
            if not row:
                conn.commit()
                return None
            job_id = row["id"]
            cur.execute(
                """
                UPDATE download_jobs
                SET status='running', running=?, updated_at=?
                WHERE id=? AND status='queued'
                """,
                (now, now, job_id),
            )
            if cur.rowcount != 1:
                conn.commit()
                return None
            conn.commit()
            data = dict(row)
            data["status"] = "running"
            data["running"] = now
            return DownloadJob.from_row(data)

    def get_job(self, job_id):
        with self._connect() as conn:
            ensure_download_jobs_table(conn)
            row = conn.execute("SELECT * FROM download_jobs WHERE id=?", (job_id,)).fetchone()
            if not row:
                return None
            return DownloadJob.from_row(row)

    def has_active_job(self, source, url):
        with self._connect() as conn:
            ensure_download_jobs_table(conn)
            row = conn.execute(
                """
                SELECT 1 FROM download_jobs
                WHERE source=? AND url=? AND status IN ('queued', 'running')
                LIMIT 1
                """,
                (source, url),
            ).fetchone()
            return row is not None

    def has_job_for_origin(self, origin, origin_id, url):
        with self._connect() as conn:
            ensure_download_jobs_table(conn)
            row = conn.execute(
                """
                SELECT 1 FROM download_jobs
                WHERE origin=? AND origin_id=? AND url=?
                LIMIT 1
                """,
                (origin, origin_id, url),
            ).fetchone()
            return row is not None

    def list_ready_sources(self, *, now=None):
        now = now or _utc_now()
        with self._connect() as conn:
            ensure_download_jobs_table(conn)
            rows = conn.execute(
                """
                SELECT DISTINCT source
                FROM download_jobs
                WHERE status='queued' AND (queued IS NULL OR queued <= ?)
                """,
                (now,),
            ).fetchall()
            return [row["source"] for row in rows]

    def next_ready_time(self, *, now=None):
        now = now or _utc_now()
        with self._connect() as conn:
            ensure_download_jobs_table(conn)
            row = conn.execute(
                """
                SELECT queued
                FROM download_jobs
                WHERE status='queued' AND queued IS NOT NULL AND queued > ?
                ORDER BY queued ASC
                LIMIT 1
                """,
                (now,),
            ).fetchone()
            if not row:
                return None
            return row["queued"]

    def mark_completed(self, job):
        now = _utc_now()
        with self._connect() as conn:
            ensure_download_jobs_table(conn)
            cur = conn.execute(
                """
                UPDATE download_jobs
                SET status='completed', completed=?, updated_at=?
                WHERE id=? AND status='running'
                """,
                (now, now, job.id),
            )
            if cur.rowcount != 1:
                return False
        _job_log(
            "info",
            trace_id=job.trace_id,
            job_id=job.id,
            source=job.source,
            event="job_completed",
            status="completed",
        )
        return True

    def mark_canceled(self, job, reason):
        now = _utc_now()
        with self._connect() as conn:
            ensure_download_jobs_table(conn)
            cur = conn.execute(
                """
                UPDATE download_jobs
                SET status='canceled', canceled=?, updated_at=?, last_error=?
                WHERE id=? AND status='running'
                """,
                (now, now, reason, job.id),
            )
            if cur.rowcount != 1:
                return False
        _job_log(
            "warning",
            trace_id=job.trace_id,
            job_id=job.id,
            source=job.source,
            event="job_canceled",
            status="canceled",
            reason=reason,
        )
        return True

    def mark_failed(self, job, *, error_message, retry_at=None, attempts=None):
        now = _utc_now()
        attempts = attempts if attempts is not None else (job.attempts + 1)
        status = "queued" if retry_at else "failed"
        with self._connect() as conn:
            ensure_download_jobs_table(conn)
            cur = conn.execute(
                """
                UPDATE download_jobs
                SET status=?, failed=COALESCE(failed, ?), queued=?, attempts=?, updated_at=?, last_error=?
                WHERE id=? AND status='running'
                """,
                (
                    status,
                    now if status == "failed" else None,
                    retry_at if status == "queued" else None,
                    attempts,
                    now,
                    error_message,
                    job.id,
                ),
            )
            if cur.rowcount != 1:
                return False
        event = "job_requeued" if status == "queued" else "job_failed"
        _job_log(
            "warning" if status == "queued" else "error",
            trace_id=job.trace_id,
            job_id=job.id,
            source=job.source,
            event=event,
            status=status,
            attempts=attempts,
            error=error_message,
            retry_at=retry_at,
        )
        return True


def _is_retryable_error(error_message):
    if not error_message:
        return False
    lowered = error_message.lower()
    non_retryable = (
        "drm",
        "http error 403",
        "http error 404",
        "403 forbidden",
        "404 not found",
        "private video",
        "video unavailable",
        "not available",
    )
    retryable = (
        "timeout",
        "timed out",
        "temporary failure",
        "connection reset",
        "connection aborted",
        "connection refused",
        "network is unreachable",
        "remote end closed connection",
        "http error 429",
        "http error 500",
        "http error 502",
        "http error 503",
        "http error 504",
        "extractor error",
        "ssl",
        "tls",
        "eof",
    )
    if any(token in lowered for token in non_retryable):
        return False
    return any(token in lowered for token in retryable)


class DownloadWorkerEngine:
    def __init__(
        self,
        db_path,
        *,
        paths,
        config,
        status=None,
        stop_event=None,
        retry_delay_seconds=None,
        poll_interval_seconds=None,
        adapters=None,
    ):
        self.store = DownloadJobStore(db_path)
        self.paths = paths
        self.config = config
        self.status = status
        self.stop_event = stop_event
        if retry_delay_seconds is not None:
            self.retry_delay_seconds = retry_delay_seconds
        elif isinstance(config, dict):
            self.retry_delay_seconds = config.get("job_retry_delay_seconds", _DEFAULT_RETRY_DELAY_SECONDS)
        else:
            self.retry_delay_seconds = _DEFAULT_RETRY_DELAY_SECONDS
        self.poll_interval_seconds = poll_interval_seconds or _DEFAULT_POLL_INTERVAL_SECONDS
        self.adapters = adapters or {}
        self._source_semaphores = {}
        self._threads = {}
        self._threads_lock = threading.Lock()
        self._yt_client_cache = {}
        self._yt_refresh_log_state = set()
        self._yt_lock = threading.Lock()

    def run_until_idle(self):
        while True:
            if self.stop_event and self.stop_event.is_set():
                break
            now = _utc_now()
            sources = self.store.list_ready_sources(now=now)
            started_any = False
            for source in sources:
                if self._start_worker(source):
                    started_any = True
            if not sources and not self._any_active_workers():
                next_ready = self.store.next_ready_time(now=now)
                if not next_ready:
                    break
                self._sleep_until(next_ready)
                continue
            if not started_any:
                time.sleep(self.poll_interval_seconds)
        self._join_workers()

    def _sleep_until(self, next_ready):
        try:
            ready_dt = datetime.fromisoformat(next_ready)
        except ValueError:
            time.sleep(self.poll_interval_seconds)
            return
        now = datetime.utcnow()
        delay = max(0.0, (ready_dt - now).total_seconds())
        time.sleep(min(self.poll_interval_seconds, delay) if delay else 0.0)

    def _start_worker(self, source):
        with self._threads_lock:
            semaphore = self._source_semaphores.get(source)
            if semaphore is None:
                semaphore = threading.Semaphore(1)
                self._source_semaphores[source] = semaphore
            if not semaphore.acquire(blocking=False):
                return False
            thread = threading.Thread(
                target=self._worker_loop,
                args=(source, semaphore),
                daemon=False,
            )
            self._threads[source] = thread
            thread.start()
            return True

    def _any_active_workers(self):
        with self._threads_lock:
            return any(thread.is_alive() for thread in self._threads.values())

    def _join_workers(self):
        with self._threads_lock:
            threads = list(self._threads.values())
        for thread in threads:
            thread.join()

    def _worker_loop(self, source, semaphore):
        try:
            while True:
                if self.stop_event and self.stop_event.is_set():
                    break
                job = self.store.claim_next(source)
                if not job:
                    break
                _job_log(
                    "info",
                    trace_id=job.trace_id,
                    job_id=job.id,
                    source=job.source,
                    event="job_running",
                    status="running",
                )
                self._execute_job(job)
        finally:
            semaphore.release()

    def _execute_job(self, job):
        if job.status != "running":
            _job_log(
                "warning",
                trace_id=job.trace_id,
                job_id=job.id,
                source=job.source,
                event="job_skipped",
                status=job.status,
                reason="status_not_running",
            )
            return
        if job.status in _TERMINAL_STATUSES:
            return
        if self.stop_event and self.stop_event.is_set():
            if self.store.mark_canceled(job, "canceled before start"):
                self._record_progress()
            return

        adapter = self.adapters.get(job.source)
        if not adapter:
            error = f"no adapter registered for source={job.source}"
            if self.store.mark_failed(job, error_message=error):
                self._record_failure(job, error)
                self._record_progress()
            return

        try:
            adapter.execute(job, self)
        except Exception as exc:
            self._handle_job_error(job, exc)

    def _record_progress(self):
        if not self.status:
            return
        from engine import core as engine_core

        current = getattr(self.status, "progress_current", 0) or 0
        total = getattr(self.status, "progress_total", 0) or 0
        current += 1
        engine_core._status_set(self.status, "progress_current", current)
        if total:
            percent = int((current / total) * 100)
            engine_core._status_set(self.status, "progress_percent", min(100, percent))

    def _handle_job_error(self, job, exc):
        error_message = str(exc) or exc.__class__.__name__
        if self.stop_event and self.stop_event.is_set():
            if self.store.mark_canceled(job, "canceled"):
                self._record_progress()
            return
        retryable = _is_retryable_error(error_message)
        attempts = job.attempts + 1
        if retryable and attempts < job.max_attempts:
            retry_at = datetime.utcnow() + timedelta(seconds=self.retry_delay_seconds)
            retry_at_str = retry_at.isoformat()
            self.store.mark_failed(
                job,
                error_message=error_message,
                retry_at=retry_at_str,
                attempts=attempts,
            )
            return
            if self.store.mark_failed(job, error_message=error_message, attempts=attempts):
                self._record_failure(job, error_message)
                self._record_progress()

    def _record_failure(self, job, error_message):
        if not self.status:
            return
        from engine import core as engine_core

        meta = job.context.get("metadata") if isinstance(job.context, dict) else None
        label = None
        if isinstance(meta, dict):
            label = meta.get("title")
        label = label or job.context.get("video_id") or job.id
        engine_core._status_append(self.status, "run_failures", label)
        engine_core._status_set(self.status, "last_error_message", error_message)
        if job.origin == "playlist":
            try:
                with sqlite3.connect(self.paths.db_path) as conn:
                    engine_core.record_playlist_error(conn, job.origin_id, error_message)
            except Exception as exc:
                _job_log(
                    "error",
                    trace_id=job.trace_id,
                    job_id=job.id,
                    source=job.source,
                    event="playlist_error_record_failed",
                    error=str(exc),
                )

    def get_youtube_client(self, account):
        if not account or not isinstance(self.config, dict):
            return None
        accounts = self.config.get("accounts", {})
        if not isinstance(accounts, dict) or account not in accounts:
            return None
        account_cfg = accounts.get(account)
        if not isinstance(account_cfg, dict):
            return None
        from engine import core as engine_core

        with self._yt_lock:
            clients = engine_core.build_youtube_clients(
                {account: account_cfg},
                self.config,
                cache=self._yt_client_cache,
                refresh_log_state=self._yt_refresh_log_state,
            )
        return clients.get(account)


class YouTubeAdapter:
    def execute(self, job, engine):
        from engine import core as engine_core

        context = dict(job.context or {})
        video_id = context.get("video_id") or engine_core.extract_video_id(job.url) or job.id
        media_audio = job.media_type == "audio"
        delivery_mode = context.get("delivery_mode") or "server"
        js_runtime = context.get("js_runtime") or engine_core.resolve_js_runtime(engine.config)
        cookies_path = context.get("cookies_path") or engine_core.resolve_cookiefile(engine.config)
        target_format = context.get("target_format")
        audio_only = bool(context.get("audio_only"))

        meta = context.get("metadata")
        if not meta:
            account = context.get("account")
            yt_client = engine.get_youtube_client(account)
            meta = engine_core.resolve_video_metadata(
                yt_client,
                video_id,
                allow_public_fallback=True,
                music_mode=media_audio,
                cookies_path=cookies_path,
            )
        if meta:
            meta["video_id"] = meta.get("video_id") or video_id

        if engine.stop_event and engine.stop_event.is_set():
            engine.store.mark_canceled(job, "canceled before download")
            return

        staging_dir = os.path.join(job.output_dir, ".staging", job.id)
        os.makedirs(staging_dir, exist_ok=True)

        engine_core._status_set(engine.status, "current_video_id", video_id)
        engine_core._status_set(engine.status, "current_video_title", meta.get("title") if meta else video_id)
        engine_core._status_set(engine.status, "current_phase", "downloading")
        engine_core._reset_video_progress(engine.status)

        local_file = engine_core.download_with_ytdlp(
            job.url,
            staging_dir,
            js_runtime,
            meta,
            engine.config,
            target_format=target_format,
            audio_only=audio_only,
            paths=engine.paths,
            status=engine.status,
            stop_event=engine.stop_event,
            music_mode=media_audio,
            cookies_path=cookies_path,
        )
        engine_core._reset_video_progress(engine.status)
        if not local_file:
            raise RuntimeError("yt-dlp download failed")

        ext = os.path.splitext(local_file)[1].lstrip(".") or (target_format or "")
        final_name = engine_core.build_output_filename(
            meta or {"title": video_id, "channel": "", "upload_date": ""},
            video_id,
            ext,
            engine.config,
            media_audio,
            template_override=job.output_template,
        )
        final_path = os.path.join(job.output_dir, final_name)
        os.makedirs(os.path.dirname(final_path), exist_ok=True)
        engine_core._status_set(engine.status, "current_phase", "finalizing")
        os.replace(local_file, final_path)
        try:
            shutil.rmtree(staging_dir, ignore_errors=True)
            staging_root = os.path.dirname(staging_dir)
            if os.path.isdir(staging_root) and not os.listdir(staging_root):
                os.rmdir(staging_root)
        except OSError:
            pass

        if delivery_mode == "client":
            delivery_id, expires_at, _event = engine_core._register_client_delivery(
                final_path,
                os.path.basename(final_name),
            )
            engine_core._status_set(engine.status, "client_delivery_id", delivery_id)
            engine_core._status_set(engine.status, "client_delivery_filename", final_name)
            engine_core._status_set(engine.status, "client_delivery_expires_at", expires_at.isoformat())
            engine_core._status_set(engine.status, "client_delivery_mode", "client")
            engine_core._status_set(engine.status, "current_phase", "ready for client download")
        else:
            engine_core._status_set(engine.status, "client_delivery_id", None)
            engine_core._status_set(engine.status, "client_delivery_filename", None)
            engine_core._status_set(engine.status, "client_delivery_expires_at", None)
            engine_core._status_set(engine.status, "client_delivery_mode", "server")
            engine_core._status_set(engine.status, "current_phase", None)

        engine_core._status_set(engine.status, "last_completed", os.path.basename(final_name))
        engine_core._status_set(engine.status, "last_completed_at", datetime.utcnow().isoformat())
        engine_core._status_set(engine.status, "last_completed_path", final_path if delivery_mode != "client" else None)
        if media_audio:
            engine_core._enqueue_music_metadata(final_path, meta or {}, engine.config, music_mode=True)

        if job.origin == "playlist":
            playlist_id = job.origin_id
            playlist_item_id = context.get("playlist_item_id")
            remove_after = bool(context.get("remove_after_download"))
            subscribe_mode = bool(context.get("subscribe_mode"))
            try:
                with sqlite3.connect(engine.paths.db_path, check_same_thread=False) as conn:
                    conn.execute(
                        "INSERT INTO downloads (video_id, playlist_id, downloaded_at, filepath) VALUES (?, ?, ?, ?)",
                        (video_id, playlist_id, datetime.utcnow(), final_path),
                    )
                    if subscribe_mode:
                        engine_core.mark_video_downloaded(conn, playlist_id, video_id)
                    conn.commit()
            except Exception as exc:
                _job_log(
                    "error",
                    trace_id=job.trace_id,
                    job_id=job.id,
                    source=job.source,
                    event="job_db_insert_failed",
                    error=str(exc),
                )

            if remove_after and playlist_item_id:
                yt_client = engine.get_youtube_client(context.get("account"))
                if yt_client:
                    try:
                        yt_client.playlistItems().delete(id=playlist_item_id).execute()
                    except Exception as exc:
                        _job_log(
                            "error",
                            trace_id=job.trace_id,
                            job_id=job.id,
                            source=job.source,
                            event="playlist_remove_failed",
                            error=str(exc),
                        )

        engine_core._status_append(engine.status, "run_successes", os.path.basename(final_name))
        engine.store.mark_completed(job)
        engine._record_progress()
        if job.origin != "playlist":
            msg = (
                "✅ Download completed → ready for client download"
                if delivery_mode == "client"
                else "✅ Download completed → saved to server library"
            )
            engine_core.telegram_notify(engine.config, msg)
