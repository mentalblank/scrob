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

export function formatSeasonTitle(
  seasonNumber: number,
  name?: string | null,
  specialsLabel = false,
): string {
  const fallback = seasonLabel(seasonNumber, specialsLabel);
  const trimDecorators = (value: string) => value.replace(/^[-–—:·\s]+|[-–—:·\s]+$/g, "").trim();
  const normalized = trimDecorators(name ?? "").replace(
    new RegExp(`^season\\s+${seasonNumber}\\s*[-–—:·]?\\s*`, "i"),
    "",
  );
  const customName = trimDecorators(normalized);
  return customName ? `${fallback} · ${customName}` : fallback;
}
