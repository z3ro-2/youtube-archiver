import io
import logging

import requests
from PIL import Image


def fetch_artwork(release_id, max_size_px=1500):
    if not release_id:
        return None
    url = f"https://coverartarchive.org/release/{release_id}/front"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            return None
    except Exception:
        logging.debug("Artwork download failed for release %s", release_id)
        return None
    content_type = response.headers.get("Content-Type", "image/jpeg")
    data = response.content
    try:
        image = Image.open(io.BytesIO(data))
        if max_size_px:
            image.thumbnail((max_size_px, max_size_px))
        output = io.BytesIO()
        fmt = "JPEG" if content_type.endswith("jpeg") or content_type.endswith("jpg") else "PNG"
        image.save(output, format=fmt)
        data = output.getvalue()
        content_type = "image/jpeg" if fmt == "JPEG" else "image/png"
    except Exception:
        logging.debug("Artwork processing failed for release %s", release_id)
        return None
    return {
        "data": data,
        "mime": content_type,
    }
