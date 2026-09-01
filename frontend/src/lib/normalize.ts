/** Normalize TMDB and TVDB show responses into a single shape. */
import type { Show, CastMember, Network, SeasonMeta, MediaItem, SeasonState } from "./api";

export interface UnifiedShow {
  id: number | null;
  uri_id: string;
  provider: "tmdb" | "tvdb";
  tmdb_id: number | null;
  tvdb_id: number | null;
  tmdb_id_cross: number | null;

  title: string;
  original_title: string | null;
  overview: string;
  tagline: string | null;
  status: string | null;

  poster_path: string | null;
  backdrop_path: string | null;
  logo_path: string | null;

  first_air_date: string | null;
  last_air_date: string | null;
  age_rating: string | null;
  original_language: string | null;
  imdb_id: string | null;
  adult: boolean;

  tmdb_rating: number | null;
  user_rating: number | null;

  genres: string[];
  networks: Network[];
  cast: CastMember[];
  recommendations: MediaItem[];

  seasons_meta: SeasonMeta[];
  seasons: Record<string, MediaItem[]>;
  season_states: Record<number, SeasonState>;

  in_library: boolean;
  watched: boolean;
  in_lists: number[];
  collection_pct: number;
  watch_pct: number;
  is_monitored: boolean;
  request_enabled: boolean;
  request_status: string | null;
  is_blocked: boolean;
  is_dropped: boolean;
  include_specials: boolean;
  watched_episodes_count: number;
  total_episodes_count: number;
  trailer_youtube_id: string | null;
  where_to_watch: { type: string; name: string; logo: string | null; is_subscribed?: boolean; category?: string; url?: string | null; connection_id?: number | null }[];
}

function isTvdbShow(s: Show): boolean {
  return (s as any).tvdb_id != null && s.tmdb_id == null;
}

function normalizeCast(cast: any[]): CastMember[] {
  return (cast ?? []).map((c) => ({
    tmdb_id: c.tmdb_id ?? c.person_id ?? null,
    name: c.name ?? "",
    character: c.character ?? "",
    profile_path: c.profile_path ?? null,
  }));
}

function normalizeNetworks(s: Show): Network[] {
  if (Array.isArray((s as any).networks)) {
    return (s as any).networks.map((n: any) => ({
      id: n.id ?? 0,
      name: n.name ?? "",
      logo_path: n.logo_path ?? null,
    }));
  }
  const single = (s as any).network;
  if (single) return [{ id: 0, name: single, logo_path: null }];
  return [];
}

function normalizeGenres(genres: any[]): string[] {
  return (genres ?? []).map((g) => (typeof g === "string" ? g : g?.name ?? ""));
}

export function normalizeShow(s: Show): UnifiedShow {
  const isTvdb = isTvdbShow(s);
  const tmdbId = s.tmdb_id ?? null;
  const tvdbId = (s as any).tvdb_id ?? null;
  const tmdbCross = (s as any).tmdb_id_cross ?? null;

  const uri_id = (s as any).uri_id ??
    (isTvdb ? `tvdb:s:${tvdbId}` : `tmdb:s:${tmdbId}`);

  return {
    id: (s as any).id ?? null,
    uri_id,
    provider: isTvdb ? "tvdb" : "tmdb",
    tmdb_id: tmdbId,
    tvdb_id: tvdbId,
    tmdb_id_cross: tmdbCross,

    title: s.title,
    original_title: (s as any).original_title ?? null,
    overview: s.overview ?? "",
    tagline: (s as any).tagline ?? null,
    status: (s as any).status ?? null,

    poster_path: s.poster_path,
    backdrop_path: s.backdrop_path,
    logo_path: (s as any).logo_path ?? null,

    first_air_date: (s as any).first_air_date ?? null,
    last_air_date: (s as any).last_air_date ?? null,
    age_rating: (s as any).age_rating ?? null,
    original_language: (s as any).original_language ?? null,
    imdb_id: (s as any).imdb_id ?? null,
    adult: (s as any).adult ?? false,

    tmdb_rating: (s as any).tmdb_rating ?? null,
    user_rating: (s as any).user_rating ?? null,

    genres: normalizeGenres((s as any).genres),
    networks: normalizeNetworks(s),
    cast: normalizeCast((s as any).cast),
    recommendations: (s as any).recommendations ?? [],

    seasons_meta: ((s as any).seasons_meta ?? (s as any).seasons ?? []) as SeasonMeta[],
    seasons: ((s as Show).seasons ?? {}) as Record<string, MediaItem[]>,
    season_states: ((s as any).season_states ?? {}) as Record<number, SeasonState>,

    in_library: (s as any).in_library ?? false,
    watched: (s as any).watched ?? false,
    in_lists: (s as any).in_lists ?? [],
    collection_pct: (s as any).collection_pct ?? 0,
    watch_pct: (s as any).watch_pct ?? 0,
    is_monitored: (s as any).is_monitored ?? false,
    request_enabled: (s as any).request_enabled ?? false,
    request_status: (s as any).request_status ?? null,
    is_blocked: (s as any).is_blocked ?? false,
    is_dropped: (s as any).is_dropped ?? false,
    include_specials: (s as any).include_specials ?? false,
    watched_episodes_count: (s as any).watched_episodes_count ?? 0,
    total_episodes_count: (s as any).total_episodes_count ?? 0,
    trailer_youtube_id: (s as any).trailer_youtube_id ?? null,
    where_to_watch: (s as any).where_to_watch ?? [],
  };
}

/** Parse URL id (e.g. "123", "tmdb:s:123") into provider, numeric ID, and canonical URI. */
export function parseShowId(rawId: string | undefined, defaultProvider: "tmdb" | "tvdb" = "tmdb"): { provider: "tmdb" | "tvdb"; numericId: number; uri: string } {
  if (!rawId) return { provider: defaultProvider, numericId: NaN, uri: "" };
  if (rawId.includes(":")) {
    const parts = rawId.split(":");
    if (parts.length === 3) {
      const provider = parts[0] === "tvdb" ? "tvdb" : "tmdb";
      return { provider, numericId: Number(parts[2]), uri: rawId };
    }
  }
  const numericId = Number(rawId);
  return { provider: defaultProvider, numericId, uri: `${defaultProvider}:s:${numericId}` };
}
