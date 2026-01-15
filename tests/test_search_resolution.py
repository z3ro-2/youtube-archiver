import os
import sqlite3
import tempfile
import unittest

from engine.job_queue import DownloadJobStore, ensure_download_jobs_table
from engine.search_adapters import SearchAdapter
from engine.search_resolution import SearchResolutionService


class FakeAdapter(SearchAdapter):
    source_name = "youtube_music"

    def __init__(self, candidates):
        self._candidates = candidates

    def search_track(self, artist, track, album=None, limit=5):
        return list(self._candidates)

    def search_album(self, artist, album, limit=5):
        return []

    def source_modifier(self, candidate):
        if candidate.get("is_official"):
            return 1.0
        return 0.90


class SearchResolutionTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.search_db_path = os.path.join(self.tmpdir.name, "search_jobs.sqlite")
        self.download_db_path = os.path.join(self.tmpdir.name, "download_jobs.sqlite")
        with sqlite3.connect(self.download_db_path) as conn:
            ensure_download_jobs_table(conn)

    def tearDown(self):
        self.tmpdir.cleanup()

    def _build_service(self, candidates):
        adapter = FakeAdapter(candidates)
        return SearchResolutionService(
            search_db_path=self.search_db_path,
            download_db_path=self.download_db_path,
            adapters={"youtube_music": adapter},
        )

    def test_resolution_enqueues_job(self):
        service = self._build_service([
            {
                "url": "https://music.example/track",
                "title": "Track Name",
                "artist": "Artist",
                "track": "Track Name",
                "is_official": True,
            }
        ])
        request_id = service.create_search_request({
            "intent": "track",
            "media_type": "audio",
            "artist": "Artist",
            "track": "Track Name",
            "source_priority_json": ["youtube_music"],
        })
        service.run_search_resolution_once(config={"final_format": "mp3"})

        with sqlite3.connect(self.search_db_path) as conn:
            row = conn.execute(
                "SELECT status FROM search_requests WHERE id=?",
                (request_id,),
            ).fetchone()
            self.assertEqual(row[0], "completed")
            item = conn.execute(
                "SELECT status, chosen_url FROM search_items WHERE request_id=?",
                (request_id,),
            ).fetchone()
            self.assertEqual(item[0], "enqueued")
            self.assertEqual(item[1], "https://music.example/track")

        with sqlite3.connect(self.download_db_path) as conn:
            jobs = conn.execute(
                "SELECT COUNT(*) FROM download_jobs WHERE origin='search' AND origin_id=?",
                (request_id,),
            ).fetchone()
            self.assertEqual(jobs[0], 1)

    def test_resolution_threshold_failure(self):
        service = self._build_service([
            {
                "url": "https://music.example/track",
                "title": "Track Name",
                "artist": "Artist",
                "track": "Track Name",
                "is_official": False,
            }
        ])
        request_id = service.create_search_request({
            "intent": "track",
            "media_type": "audio",
            "artist": "Artist",
            "track": "Track Name",
            "min_match_score": 0.98,
            "source_priority_json": ["youtube_music"],
        })
        service.run_search_resolution_once()

        with sqlite3.connect(self.search_db_path) as conn:
            item = conn.execute(
                "SELECT status, error FROM search_items WHERE request_id=?",
                (request_id,),
            ).fetchone()
            self.assertEqual(item[0], "failed")
            self.assertEqual(item[1], "no_candidate_above_threshold")
            req = conn.execute(
                "SELECT status, error FROM search_requests WHERE id=?",
                (request_id,),
            ).fetchone()
            self.assertEqual(req[0], "failed")

    def test_enqueue_idempotency(self):
        candidate = {
            "url": "https://music.example/track",
            "title": "Track Name",
            "artist": "Artist",
            "track": "Track Name",
            "is_official": True,
        }
        service = self._build_service([candidate])
        request_id = service.create_search_request({
            "intent": "track",
            "media_type": "audio",
            "artist": "Artist",
            "track": "Track Name",
            "source_priority_json": ["youtube_music"],
        })
        store = DownloadJobStore(self.download_db_path)
        store.enqueue(
            origin="search",
            origin_id=request_id,
            media_type="audio",
            media_intent="track",
            source="youtube_music",
            url=candidate["url"],
            output_template=None,
            output_dir="/tmp",
        )
        service.run_search_resolution_once(config={"final_format": "mp3"})

        with sqlite3.connect(self.download_db_path) as conn:
            jobs = conn.execute(
                "SELECT COUNT(*) FROM download_jobs WHERE origin='search' AND origin_id=? AND url=?",
                (request_id, candidate["url"]),
            ).fetchone()
            self.assertEqual(jobs[0], 1)

