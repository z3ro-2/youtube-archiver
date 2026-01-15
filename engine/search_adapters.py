class SearchAdapter:
    source_name = ""

    def search_track(self, artist, track, album=None, limit=5):
        return []

    def search_album(self, artist, album, limit=5):
        return []

    def expand_album_to_tracks(self, candidate_album):
        return None

    def source_modifier(self, candidate):
        return 1.0


class BandcampAdapter(SearchAdapter):
    source_name = "bandcamp"

    def source_modifier(self, candidate):
        return 1.05


class YouTubeMusicAdapter(SearchAdapter):
    source_name = "youtube_music"

    def source_modifier(self, candidate):
        if candidate.get("is_official"):
            return 1.0
        return 0.90


class SoundCloudAdapter(SearchAdapter):
    source_name = "soundcloud"

    def source_modifier(self, candidate):
        return 0.95
