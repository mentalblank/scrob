# Emby and Jellyfin share the same REST API — all functions are re-exported.
from core.jellyfin import (
    validate_connection,
    get_libraries,
    get_movies,
    get_shows,
    get_episodes,
    get_item,
    extract_quality,
    find_movie_by_tmdb_id,
    find_episode_by_ids,
    mark_watched,
    mark_unwatched,
    set_rating,
)
