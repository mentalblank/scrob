from __future__ import annotations

import re
from dataclasses import dataclass

_PATTERN = re.compile(r'^([a-z]+):([sme]):(\d+)$')

TYPE_PREFIX_TO_MEDIA_TYPE: dict[str, str] = {
    's': 'series',
    'm': 'movie',
    'e': 'episode',
}

MEDIA_TYPE_TO_PREFIX: dict[str, str] = {v: k for k, v in TYPE_PREFIX_TO_MEDIA_TYPE.items()}


@dataclass(frozen=True)
class MediaURI:
    provider: str
    type_prefix: str  # 's' = series, 'm' = movie, 'e' = episode
    id: str

    @property
    def is_internal(self) -> bool:
        return self.provider == "internal"

    @property
    def media_type(self) -> str:
        return TYPE_PREFIX_TO_MEDIA_TYPE[self.type_prefix]

    def __str__(self) -> str:
        return f"{self.provider}:{self.type_prefix}:{self.id}"

    @classmethod
    def parse(cls, uri: str) -> MediaURI:
        m = _PATTERN.match(uri)
        if not m:
            raise ValueError(f"Invalid MediaURI: {uri!r}. Expected format: provider:type:id (e.g. tmdb:s:123)")
        return cls(provider=m.group(1), type_prefix=m.group(2), id=m.group(3))

    @classmethod
    def for_show(cls, provider: str, external_id: int) -> MediaURI:
        return cls(provider=provider, type_prefix='s', id=str(external_id))

    @classmethod
    def for_movie(cls, provider: str, external_id: int) -> MediaURI:
        return cls(provider=provider, type_prefix='m', id=str(external_id))

    @classmethod
    def for_episode(cls, provider: str, external_id: int) -> MediaURI:
        return cls(provider=provider, type_prefix='e', id=str(external_id))
