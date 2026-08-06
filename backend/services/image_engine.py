import hashlib
import io
import os
from typing import Any


class ImageEngine:
    SIZE_MAP = {
        "small": 300,
        "medium": 600,
        "original": None,
    }

    @staticmethod
    def generate_etag(data: bytes) -> str:
        sha = hashlib.sha256(data).hexdigest()
        return f'"{sha[:16]}"'

    @staticmethod
    def should_return_304(client_etag: str | None, current_etag: str) -> bool:
        if not client_etag:
            return False
        clean_client = client_etag.strip('"')
        clean_current = current_etag.strip('"')
        return clean_client == clean_current

    @staticmethod
    async def fetch_and_optimize_image(
        image_url: str, size: str = "medium", format: str = "webp"
    ) -> dict[str, Any]:
        # Generate deterministic cached image path
        url_hash = hashlib.md5(image_url.encode()).hexdigest()
        filename = f"{url_hash}_{size}.{format}"
        cached_path = os.path.join("/tmp", "image_cache", filename)

        # Simulate optimization & ETag generation
        dummy_content = f"OptimizedImage_{image_url}_{size}_{format}".encode()
        etag = ImageEngine.generate_etag(dummy_content)

        return {
            "image_url": image_url,
            "size": size,
            "format": format,
            "cached_path": cached_path,
            "etag": etag,
            "content_type": f"image/{format}",
            "cache_control": "public, max-age=31536000, immutable",
            "bytes_size": len(dummy_content),
        }
