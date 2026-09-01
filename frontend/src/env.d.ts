/// <reference path="../.astro/types.d.ts" />
/// <reference types="astro/client" />

declare namespace App {
  interface Locals {
    user: import("./lib/api").UserProfile | null;
    token: string | undefined;
  }
}

// Base.astro installs these on window so every page can call them without
// importing; declare them so the call sites type-check.
interface Window {
  showToast(message: string, type?: "success" | "error" | "info"): void;
  showConfirm(title: string, body: string): Promise<boolean>;
  showMessage(text: string, type?: "success" | "error"): void;

  // Values Base.astro passes into its define:vars script, and helpers other
  // pages reach for through window rather than importing.
  __AUTH_TOKEN__?: string;
  __USE_24H__?: boolean;
  __IS_ADMIN__?: boolean;
  __RADARR_CUSTOMIZE_ON_ADD__?: boolean;
  __SONARR_CUSTOMIZE_ON_ADD__?: boolean;
  esc?: (value: unknown) => string;
  hasHistoryCardDeleteListener?: boolean;
  buildItemUrl(item: any, preferTvdb?: boolean): string;
  openWatchHistoryModal(
    btn: HTMLElement | null,
    uriId: string | null,
    mediaType: string,
    mediaId?: string | null,
    showUri?: string | null,
    season?: string | null,
    episode?: string | null,
  ): Promise<void>;
  openRemapModal(uriOrTmdbId: string | number, season: number, title: string): void;
  openMatchModal(seriesName: string): void;
  openMatchMovieModal(movieTitle: string): void;
}
