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
    if (!preferTvdb && parsed.provider === "tvdb" && tmdb_id != null) return `/show/tmdb:s:${tmdb_id}`;
    return `/show/${parsed.provider}:s:${parsed.id}`;
  }
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
  const tmdbValid = tmdb_id != null && tmdb_id !== 'null';
  const tvdbValid = tvdb_id != null && tvdb_id !== 'null';

  if (tvdbValid && (!tmdbValid || preferTvdb)) return `/person/tvdb:p:${tvdb_id}`;
  if (tmdbValid) return `/person/tmdb:p:${tmdb_id}`;
  return "/";
}

/** Canonical URL for an episode item's parent show + S/E path. */
export function episodeUrl(item: MinimalItem, preferTvdb = false): string {
  const sn = item.season_number;
  const en = item.episode_number;
  const showTvdb = item.show_tvdb_id ?? null;
  const showTmdb = item.show_tmdb_id ?? null;

  if (preferTvdb && showTvdb != null && sn != null && en != null) {
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
    
    if (preferTvdb && showTvdb != null && sn != null) {
      return `/show/tvdb:s:${showTvdb}/season/${sn}`;
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
