import unittest

from engine.search_scoring import duration_score, normalize_text, rank_candidates, score_candidate, tokenize


class SearchScoringTests(unittest.TestCase):
    def test_normalize_text(self):
        value = "Song Title (Official Video) [Lyrics] feat. Artist!"
        normalized = normalize_text(value)
        self.assertEqual(normalized, "song title feat artist")
        self.assertEqual(tokenize("AC/DC & Friends"), ["ac/dc", "&", "friends"])

    def test_duration_score_curve(self):
        self.assertEqual(duration_score(100, 102), 1.0)
        self.assertEqual(duration_score(100, 104), 0.90)
        self.assertEqual(duration_score(100, 108), 0.75)
        self.assertEqual(duration_score(100, 112), 0.50)
        self.assertEqual(duration_score(100, 130), 0.20)

    def test_penalty_cover(self):
        target = {"artist": "Daft Punk", "track": "Harder Better Faster Stronger", "album": ""}
        candidate = {
            "artist": "Daft Punk",
            "track": "Harder Better Faster Stronger",
            "title": "Harder Better Faster Stronger (cover)",
        }
        breakdown = score_candidate(target, candidate, source_modifier=1.0)
        self.assertEqual(breakdown.penalty_multiplier, 0.10)
        self.assertLess(breakdown.final_score, breakdown.weighted_sum)

    def test_rank_candidates(self):
        target = {"artist": "Artist", "track": "Track", "album": ""}
        candidates = [
            {
                "source": "bandcamp",
                "url": "https://example.com/a",
                "title": "Track",
                "artist": "Artist",
                "track": "Track",
                "source_modifier": 1.05,
            },
            {
                "source": "soundcloud",
                "url": "https://example.com/b",
                "title": "Track",
                "artist": "Artist",
                "track": "Track",
                "source_modifier": 0.95,
            },
        ]
        ranked = rank_candidates(target, candidates, source_priority=["bandcamp", "soundcloud"])
        self.assertEqual(ranked[0][0]["url"], "https://example.com/a")
        self.assertEqual(ranked[0][2], 1)
        self.assertEqual(ranked[1][2], 2)

