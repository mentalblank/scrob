/** Centralized URL construction, preferring uri_id over integer IDs. */

interface MinimalItem {
  type?: string;
  tmdb_id?: number | null;
  tvdb_id?: number | null;
  uri_id?: string | null;
  show_tmdb_id?: number | null;
  show_tvdb_id?: number | null;
  show_uri_id?: string | null;
  season_number?: number | null;
  episode_number?: number | null;
  // The show's resolved numbering preference and, when it is "tvdb", this
  // item's position in TVDB's numbering (attached by the backend's
  // _attach_episode_order_fields). The two catalogues number plenty of shows
  // differently, so a TVDB show id must never be paired with a TMDB position.
  show_episode_order?: "tmdb" | "tvdb" | null;
  tvdb_season_number?: number | null;
  tvdb_episode_number?: number | null;
  tvdb_sourced?: boolean;
}

/** Parse "provider:type:id" → { provider, typePrefix, id } or null. */
function parseUri(uri: string | null | undefined): { provider: string; prefix: string; id: string } | null {
  if (!uri) return null;
  const parts = uri.split(":");
  if (parts.length !== 3) return null;
  return { provider: parts[0], prefix: parts[1], id: parts[2] };
}

/** Canonical URL for a show, given its uri_id, tvdb_id, or tmdb_id. */
export function showUrl(
  uri_id: string | null | undefined,
  tvdb_id: number | null | undefined,
  tmdb_id: number | null | undefined,
  preferTvdb = false
): string {
  if (preferTvdb && tvdb_id != null) return `/show/tvdb:s:${tvdb_id}`;
  const parsed = parseUri(uri_id);
  if (parsed?.prefix === "s") {
    if (preferTvdb && tvdb_id != null) return `/show/tvdb:s:${tvdb_id}`;
    if (!preferTvdb && parsed.provider === "tvdb" && tmdb_id != null) return `/show/tmdb:s:${tmdb_id}`;
    return `/show/${parsed.provider}:s:${parsed.id}`;
  }
  if (preferTvdb && tvdb_id != null) return `/show/tvdb:s:${tvdb_id}`;
  if (tmdb_id != null) return `/show/tmdb:s:${tmdb_id}`;
  if (tvdb_id != null) return `/show/tvdb:s:${tvdb_id}`;
  return "/shows";
}

/** URL for person page (TMDB or TVDB). */
export function personUrl(
  tmdb_id: any,
  tvdb_id: any,
  preferTvdb = false,
): string {
  const tmdbValid = tmdb_id != null && tmdb_id !== 'null' && tmdb_id !== '';
  const tvdbValid = tvdb_id != null && tvdb_id !== 'null' && tvdb_id !== '';

  if (tvdbValid && (!tmdbValid || preferTvdb)) return `/person/tvdb:p:${tvdb_id}`;
  if (tmdbValid) return `/person/tmdb:p:${tmdb_id}`;
  if (tvdbValid) return `/person/tvdb:p:${tvdb_id}`;
  return "/";
}

/** Canonical URL for an episode item's parent show + S/E path. */
export function episodeUrl(item: MinimalItem, preferTvdb = false): string {
  const sn = item.season_number;
  const en = item.episode_number;
  const showTvdb = item.show_tvdb_id ?? null;
  const showTmdb = item.show_tmdb_id ?? null;

  // tvdb_sourced episodes have no TMDB counterpart, so sn/en are already
  // TVDB-native; everything else needs the translated position before it can
  // be used with a TVDB show id.
  if (preferTvdb && showTvdb != null && !item.tvdb_sourced) {
    if (item.tvdb_season_number != null && item.tvdb_episode_number != null) {
      return `/show/tvdb:s:${showTvdb}/season/${item.tvdb_season_number}/${item.tvdb_episode_number}`;
    }
    // No mapping for this episode: TMDB's numbers are meaningless against a
    // TVDB id, so fall through to the TMDB route rather than link to whatever
    // episode happens to sit at those numbers on TVDB.
  } else if (preferTvdb && showTvdb != null && sn != null && en != null) {
    return `/show/tvdb:s:${showTvdb}/season/${sn}/${en}`;
  }

  // Resolve show identity from show_uri_id first
  const parsed = parseUri(item.show_uri_id ?? item.uri_id);
  const isShowUri = parsed?.prefix === "s";

  if (isShowUri && sn != null && en != null) {
    if (!preferTvdb && parsed?.provider === "tvdb" && showTmdb != null) {
      return `/show/tmdb:s:${showTmdb}/season/${sn}/${en}`;
    }
    return `/show/${parsed!.provider}:s:${parsed!.id}/season/${sn}/${en}`;
  }

  if (sn != null && en != null) {
    if (showTmdb) return `/show/tmdb:s:${showTmdb}/season/${sn}/${en}`;
    if (showTvdb) return `/show/tvdb:s:${showTvdb}/season/${sn}/${en}`;
  }
  if (preferTvdb && showTvdb) return `/show/tvdb:s:${showTvdb}`;
  if (showTmdb) return `/show/tmdb:s:${showTmdb}`;
  if (showTvdb) return `/show/tvdb:s:${showTvdb}`;
  return "/shows";
}

/** Canonical URL for any media item. */
export function mediaUrl(item: MinimalItem, preferTvdb = false): string {
  const t = item.type;

  if (t === "movie") {
    if (item.uri_id) return `/media/movie/${item.uri_id}`;
    if (item.tmdb_id) return `/media/movie/tmdb:m:${item.tmdb_id}`;
    return "/movies";
  }

  if (t === "series") {
    return showUrl(item.uri_id, item.tvdb_id, item.tmdb_id, preferTvdb);
  }

  if (t === "season") {
    const sn = item.season_number;
    const showTvdb = item.show_tvdb_id ?? null;
    const showTmdb = item.show_tmdb_id ?? null;
    
    if (preferTvdb && showTvdb != null) {
      if (item.tvdb_season_number != null) {
        return `/show/tvdb:s:${showTvdb}/season/${item.tvdb_season_number}`;
      }
      if (item.tvdb_sourced && sn != null) {
        return `/show/tvdb:s:${showTvdb}/season/${sn}`;
      }
    }
    
    const parsed = parseUri(item.show_uri_id ?? item.uri_id);
    const isShowUri = parsed?.prefix === "s";
    if (isShowUri && sn != null) {
      if (!preferTvdb && parsed?.provider === "tvdb" && showTmdb != null) {
         return `/show/tmdb:s:${showTmdb}/season/${sn}`;
      }
      return `/show/${parsed!.provider}:s:${parsed!.id}/season/${sn}`;
    }
    if (sn != null) {
      if (showTmdb) return `/show/tmdb:s:${showTmdb}/season/${sn}`;
      if (showTvdb) return `/show/tvdb:s:${showTvdb}/season/${sn}`;
    }
    return "/shows";
  }

  if (t === "episode") return episodeUrl(item, preferTvdb);

  if (t === "person") {
    return personUrl(item.tmdb_id, item.tvdb_id, preferTvdb);
  }

  if (t === "collection") return item.tmdb_id ? `/collection/${item.tmdb_id}` : "/";

  return item.tmdb_id ? `/media/${t}/${item.tmdb_id}` : "/";
}
