import asyncio
import pytest
from services.image_engine import ImageEngine


def test_generate_etag_and_304_matching():
    content = b"test_poster_image_bytes"
    etag = ImageEngine.generate_etag(content)

    assert etag.startswith('"') and etag.endswith('"')

    # Matching client ETag -> 304 Not Modified
    assert ImageEngine.should_return_304(etag, etag) is True
    assert ImageEngine.should_return_304(etag.strip('"'), etag) is True

    # Mismatched client ETag -> 200 OK
    assert ImageEngine.should_return_304('"different_etag"', etag) is False
    assert ImageEngine.should_return_304(None, etag) is False


def test_fetch_and_optimize_image():
    image_url = "https://image.tmdb.org/t/p/original/poster123.jpg"
    res = asyncio.run(
        ImageEngine.fetch_and_optimize_image(image_url, size="small", format="webp")
    )

    assert res["image_url"] == image_url
    assert res["size"] == "small"
    assert res["format"] == "webp"
    assert res["content_type"] == "image/webp"
    assert "public, max-age=31536000" in res["cache_control"]
    assert res["cached_path"].endswith("_small.webp")
    assert "etag" in res
