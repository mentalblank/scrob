// Season 0 is where every catalogue files specials. Users who think of them that
// way can opt into the label via the "Label Season 0 as Specials" setting, which
// rides along as a cookie so server-rendered cards can read it.
export function isSpecialsSeason(seasonNumber: number | null | undefined): boolean {
  return seasonNumber === 0;
}

export function seasonLabel(seasonNumber: number | null | undefined, specialsLabel = false): string {
  if (specialsLabel && isSpecialsSeason(seasonNumber)) return "Specials";
  return `Season ${seasonNumber}`;
}

/** Short form used in breadcrumbs and season pagers: S3, or SP for specials. */
export function seasonCode(seasonNumber: number | null | undefined, specialsLabel = false): string {
  if (specialsLabel && isSpecialsSeason(seasonNumber)) return "SP";
  return `S${seasonNumber}`;
}

/**
 * Season name for display, where a provider supplied one.
 *
 * TMDB and TVDB both hand back "Season 0" as the name, so preferring the provider
 * name outright would mean the Specials setting never showed up on those pages.
 */
export function seasonDisplayName(
  seasonNumber: number | null | undefined,
  name?: string | null,
  specialsLabel = false,
): string {
  if (specialsLabel && isSpecialsSeason(seasonNumber)) return "Specials";
  return name || `Season ${seasonNumber}`;
}

export function episodeCode(
  seasonNumber: number | null | undefined,
  episodeNumber: number | null | undefined,
  specialsLabel = false,
): string | null {
  if (seasonNumber == null || episodeNumber == null) return null;
  const ep = String(episodeNumber).padStart(2, "0");
  if (specialsLabel && isSpecialsSeason(seasonNumber)) return `SP${ep}`;
  return `S${String(seasonNumber).padStart(2, "0")}E${ep}`;
}

/** An item's season/episode as the viewer's chosen catalogue numbers it.
 *
 * The backend attaches tvdb_season_number/tvdb_episode_number only for shows
 * that actually resolve to TVDB numbering (per-show override, or the account's
 * primary metadata source - see backend/routers/media.py's
 * _attach_episode_order_fields), so their presence is the signal on its own.
 * Without this a card captions a TVDB link with TMDB's numbers: Re:ZERO's
 * "S01E79" for the episode TVDB calls S04E13.
 */
export interface EpisodePositionItem {
  season_number?: number | null;
  episode_number?: number | null;
  tvdb_season_number?: number | null;
  tvdb_episode_number?: number | null;
  // Already TVDB-native numbers - never translated, nothing to swap.
  tvdb_sourced?: boolean;
}

export function displayEpisodeCode(
  item: EpisodePositionItem,
  specialsLabel = false,
): string | null {
  if (
    !item.tvdb_sourced &&
    item.tvdb_season_number != null &&
    item.tvdb_episode_number != null
  ) {
    return episodeCode(item.tvdb_season_number, item.tvdb_episode_number, specialsLabel);
  }
  return episodeCode(item.season_number, item.episode_number, specialsLabel);
}

export function formatSeasonTitle(
  seasonNumber: number,
  name?: string | null,
  specialsLabel = false,
): string {
  const fallback = seasonLabel(seasonNumber, specialsLabel);
  const trimDecorators = (value: string) => value.replace(/^[-–—:·\s]+|[-–—:·\s]+$/g, "").trim();
  // Strip a redundant leading label ("Season 3 - ", or "Specials - " for season 0)
  // so a name that only repeats the fallback collapses back to it.
  const prefixRe = isSpecialsSeason(seasonNumber)
    ? /^(?:season\s+0|specials)\s*[-–—:·]?\s*/i
    : new RegExp(`^season\\s+${seasonNumber}\\s*[-–—:·]?\\s*`, "i");
  const customName = trimDecorators(trimDecorators(name ?? "").replace(prefixRe, ""));
  return customName ? `${fallback} · ${customName}` : fallback;
}


/** "3 episodes left · ~2h 10m" for a Next Up card; null when nothing is left. */
export function formatEpisodesLeft(
  episodesLeft?: number | null,
  remainingRuntime?: number | null,
): string | null {
  if (episodesLeft == null || episodesLeft <= 0) return null;
  const label = `${episodesLeft} episode${episodesLeft === 1 ? "" : "s"} left`;
  if (!remainingRuntime || remainingRuntime <= 0) return label;
  const h = Math.floor(remainingRuntime / 60);
  const m = remainingRuntime % 60;
  const runtime = h > 0 ? (m > 0 ? `~${h}h ${m}m` : `~${h}h`) : `~${m}m`;
  return `${label} · ${runtime}`;
}
