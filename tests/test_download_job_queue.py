import os
import sqlite3
import tempfile
import time
import unittest
from datetime import datetime

from engine.job_queue import DownloadJobStore, ensure_download_jobs_table


class DownloadJobQueueTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmpdir.name, "db.sqlite")
        with sqlite3.connect(self.db_path) as conn:
            ensure_download_jobs_table(conn)
        self.store = DownloadJobStore(self.db_path)

    def tearDown(self):
        self.tmpdir.cleanup()

    def _enqueue_job(self, url):
        return self.store.enqueue(
            origin="playlist",
            origin_id="PL123",
            media_type="video",
            media_intent="playlist",
            source="youtube",
            url=url,
            output_template=None,
            output_dir="/tmp",
        )

    def test_enqueue_and_claim_fifo(self):
        job_a = self._enqueue_job("https://example.com/a")
        time.sleep(0.01)
        job_b = self._enqueue_job("https://example.com/b")

        claimed = self.store.claim_next("youtube")
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed.id, job_a)
        self.assertEqual(claimed.status, "running")
        self.assertTrue(self.store.has_active_job("youtube", "https://example.com/b"))
        self.assertTrue(self.store.has_active_job("youtube", "https://example.com/a"))

        next_job = self.store.claim_next("youtube")
        self.assertIsNotNone(next_job)
        self.assertEqual(next_job.id, job_b)

    def test_requeue_updates_attempts(self):
        job_id = self._enqueue_job("https://example.com/a")
        job = self.store.claim_next("youtube")
        retry_at = datetime.utcnow().isoformat()
        self.assertTrue(self.store.mark_failed(job, error_message="timeout", retry_at=retry_at))

        refreshed = self.store.get_job(job_id)
        self.assertEqual(refreshed.status, "queued")
        self.assertEqual(refreshed.attempts, 1)
        self.assertEqual(refreshed.queued, retry_at)
