const BACKEND_PORT = (import.meta.env.BACKEND_PORT as string | undefined) ?? "7331";
const BASE = `http://localhost:${BACKEND_PORT}`;

type ParamValue = string | number | boolean | undefined;

async function request<T>(
  path: string,
  method: string = "GET",
  params?: Record<string, ParamValue | ParamValue[]>,
  body?: unknown,
  token?: string
): Promise<T> {
  const url = new URL(path.startsWith("/") ? path.slice(1) : path, BASE.endsWith("/") ? BASE : BASE + "/");
  if (params) {
    Object.entries(params).forEach(([k, v]) => {
      if (v === undefined) return;
      if (Array.isArray(v)) {
        v.forEach((item) => { if (item !== undefined) url.searchParams.append(k, String(item)); });
      } else {
        url.searchParams.set(k, String(v));
      }
    });
  }

  const headers: Record<string, string> = {};
  if (token) {
    headers["Authorization"] = `Bearer ${token}`;
  }

  let finalBody: BodyInit | undefined;
  if (body instanceof FormData) {
    finalBody = body;
  } else if (body !== undefined) {
    headers["Content-Type"] = "application/json";
    finalBody = JSON.stringify(body);
  }

  const res = await fetch(url.toString(), {
    method,
    headers,
    body: finalBody,
  });

  if (!res.ok) {
    let errorDetail = "";
    try {
      const errorJson = await res.json();
      errorDetail = errorJson.detail || JSON.stringify(errorJson);
    } catch (e) { }
    const err = new Error(`API ${res.status}: ${path} ${errorDetail}`);
    // Callers need to tell "your session is invalid" apart from "the backend
    // hiccuped" — the middleware must not log a user out over a 502.
    (err as any).status = res.status;
    throw err;
  }
  return res.json();
}

async function get<T>(path: string, params?: Record<string, ParamValue | ParamValue[]>, token?: string): Promise<T> {
  return request<T>(path, "GET", params, undefined, token);
}

async function post<T>(path: string, body?: unknown, token?: string): Promise<T> {
  return request<T>(path, "POST", undefined, body, token);
}

async function put<T>(path: string, body?: unknown, token?: string): Promise<T> {
  return request<T>(path, "PUT", undefined, body, token);
}

async function patch<T>(path: string, body?: unknown, token?: string): Promise<T> {
  return request<T>(path, "PATCH", undefined, body, token);
}

async function del<T>(path: string, params?: Record<string, string | number | boolean | undefined>, token?: string): Promise<T> {
  return request<T>(path, "DELETE", params, undefined, token);
}

// Shared sub-types

export type ImageKind = "poster" | "backdrop" | "still";
export type ImageSource = "tmdb" | "tvdb" | "external";

export interface ImageOption {
  source: ImageSource;
  kind: ImageKind;
  /** Provider path, stored verbatim when this option is chosen. */
  path: string;
  /** Preview value, ready for tmdbImageUrl. */
  url: string;
  language: string | null;
  width: number | null;
  height: number | null;
  votes: number | null;
}

export interface ImageOptionsResponse {
  target: "show" | "season" | "episode" | "movie";
  kind: ImageKind;
  options: ImageOption[];
}

export interface ImageOverride {
  id: number;
  subject_uri: string;
  season_number: number | null;
  episode_number: number | null;
  kind: ImageKind;
  source: ImageSource;
  source_url: string | null;
  url: string;
}

export interface ImageOverrideInput {
  subject_uri: string;
  kind: ImageKind;
  source: ImageSource;
  season_number?: number;
  episode_number?: number;
  path?: string;
  url?: string;
}

export interface CastMember {
  tmdb_id: number | null;
  // TVDB-sourced cast carries no tmdb_id — the person id arrives under these.
  tvdb_id?: number | null;
  person_id?: number | null;
  uri_id?: string | null;
  name: string;
  character: string;
  profile_path: string | null;
}

export interface Network {
  id: number;
  name: string;
  logo_path: string | null;
}

export interface ProductionCompany {
  id: number;
  name: string;
  logo_path: string | null;
}

/** Where a season came from when it exists on this show only through a remap. */
export interface RemappedFrom {
  show_uri_id: string | null;
  show_title: string;
  season_number: number;
}

export interface SeasonMeta {
  season_number: number;
  name: string;
  overview: string | null;
  poster_path: string | null;
  episode_count: number;
  air_date: string | null;
  tmdb_season_id?: number | null;
  tmdb_rating?: number | null;
  remapped_from?: RemappedFrom | null;
}

export interface SeasonState {
  watched: boolean;
  in_library: boolean;
  collection_pct: number;
  watch_pct: number;
  watch_started?: boolean;
  user_rating: number | null;
  watched_episodes_count?: number;
  total_episodes_count?: number;
}

export interface EpisodeItem {
  id: number | null;
  tmdb_id: number | null;
  uri_id?: string | null;
  episode_number: number;
  title: string;
  overview: string | null;
  air_date: string | null;
  poster_path: string | null;
  tmdb_rating: number;
  runtime: number | null;
  in_library: boolean;
  watched: boolean;
  user_rating: number | null;
  in_lists: number[];
  progress_percent?: number;
}

export interface ShowSummary {
  id?: number | null;
  tmdb_id: number | null;
  tvdb_id?: number | null;
  uri_id?: string | null;
  title: string;
  custom_title?: string | null;
  poster_path: string | null;
  backdrop_path: string | null;
  seasons_meta: SeasonMeta[];
  adult?: boolean;
}

export interface Season {
  show: ShowSummary;
  name: string;
  overview: string | null;
  poster_path: string | null;
  backdrop_path: string | null;
  air_date: string | null;
  tmdb_rating?: number | null;
  episodes: EpisodeItem[];
  season_number: number;
  remapped_from?: RemappedFrom | null;
  is_blocked: boolean;
  is_dropped: boolean;
  show_watched: boolean;
  season_watched: boolean;
  season_watch_pct: number;
  season_watch_started?: boolean;
  season_in_library: boolean;
  season_collection_pct: number;
  season_user_rating: number | null;
  show_in_lists: number[];
  show_in_library: boolean;
  show_collection_pct: number;
  show_request_enabled: boolean;
  show_request_status?: string | null;
  show_is_monitored: boolean;
}

export interface EpisodeDetail {
  show: ShowSummary;
  remapped_from?: RemappedFrom | null;
  title: string;
  overview: string | null;
  still_path: string | null;
  air_date: string | null;
  episode_number: number;
  season_number: number;
  runtime: number | null;
  tmdb_rating: number | null;
  tmdb_id: number | null;
  tvdb_id?: number | null;
  uri_id?: string | null;
  id: number | null;
  in_library: boolean;
  watched: boolean;
  in_lists: number[];
  collection_pct: number;
  user_rating?: number | null;
  play_count?: number;
  progress_percent?: number;
  is_blocked: boolean;
  is_dropped: boolean;
  cast: CastMember[];
  guest_stars: CastMember[];
  episodes: EpisodeItem[];
  season?: {
    name: string;
    season_number: number;
    poster_path: string | null;
  };
  library: {
    resolution: string;
    video_codec: string;
    audio_codec: string;
    audio_channels: string;
    audio_languages: string[];
    subtitle_languages: string[];
  } | null;
  request_enabled?: boolean;
  request_status?: string | null;
}

export interface PersonCredit {
  tmdb_id: number | null;
  tvdb_id?: number | null;
  type: "movie" | "series";
  title: string;
  poster_path: string | null;
  release_date: string | null;
  character: string | null;
  watched?: boolean;
  in_lists?: number[];
  in_library?: boolean;
  collection_pct?: number;
}

export interface PersonDetail {
  tmdb_id: number | null;
  uri_id?: string | null;
  tvdb_id?: number | null;
  name: string;
  profile_path: string | null;
  known_for_department: string | null;
  biography: string | null;
  birthday: string | null;
  place_of_birth: string | null;
  credits: PersonCredit[];
  total_credits: number;
  page: number;
  page_size: number;
  in_lists: number[];
  collection: "in" | "out" | null;
}

export interface WatchEvent {
  id: number;
  media: MediaItem;
  user_id: number;
  watched_at: string | null;
  completed: boolean;
  progress_percent: number | null;
}

export interface SyncJob {
  id: number;
  source: string;
  status: string;
  total_items: number;
  processed_items: number;
  error_message: string | null;
  updated_at: string;
}

export interface ShowSeasonOverride {
  id: number;
  source_show_id: number;
  source_show_uri_id: string | null;
  source_season_number: number;
  source_show_title?: string | null;
  source_show_poster_path?: string | null;
  target_show_id: number | null;
  target_show_uri_id: string | null;
  target_show_tmdb_id?: number | null;
  target_season_number: number;
  target_show_title?: string | null;
  target_show_poster_path?: string | null;
}

export interface UserList {
  id: number;
  name: string;
  description: string | null;
  privacy_level: PrivacyLevel;
  item_count: number;
  created_at: string;
  updated_at: string;
  preview_posters: { url: string; adult: boolean }[];

  radarr_auto_add: boolean;
  radarr_root_folder: string | null;
  radarr_quality_profile: number | null;
  radarr_tags: number[] | null;
  radarr_monitor: string | null;

  sonarr_auto_add: boolean;
  sonarr_root_folder: string | null;
  sonarr_quality_profile: number | null;
  sonarr_tags: number[] | null;
  sonarr_series_type: string | null;
  sonarr_season_folder: boolean;
  sonarr_monitor: string | null;
}

export interface PublicList extends UserList {
  username: string;
}

export interface ListItemEntry {
  id: number;
  list_id: number;
  added_at: string;
  sort_order: number;
  notes: string | null;
  media: MediaItem;
}

export interface ListDetail extends UserList {
  items: ListItemEntry[];
  is_owner: boolean;
}

// Main types

export type MediaType = "movie" | "series" | "season" | "episode" | "person" | "collection";

export interface UserProfile {
  id: number;
  email: string;
  username: string;
  display_name: string;
  role: string;
  is_admin: boolean;
  avatar_url: string | null;
  api_key: string;
  totp_enabled: boolean;
  has_password?: boolean;
  plex_linked?: boolean;
  plex_username?: string | null;
  created_at: string;
  needs_setup?: boolean;
  needs_onboarding?: boolean;
  // Explicit IANA zone, or null when the user has never picked one.
  timezone?: string | null;
}

export interface AdminUser {
  id: number;
  username: string;
  email: string;
  is_admin: boolean;
  api_key: string;
  created_at: string;
  avatar_url: string | null;
}

export interface GlobalSettings {
  tmdb_api_key: string | null;
  tvdb_api_key: string | null;
  radarr_url: string | null;
  radarr_token: string | null;
  radarr_root_folder: string | null;
  radarr_quality_profile: number | null;
  radarr_tags: number[] | null;
  sonarr_url: string | null;
  sonarr_token: string | null;
  sonarr_root_folder: string | null;
  sonarr_quality_profile: number | null;
  sonarr_tags: number[] | null;
  sonarr_season_folder: boolean;
  radarr_require_approval: boolean;
  sonarr_require_approval: boolean;
  image_cache_enabled: boolean;
  image_cache_limit_gb: number | null;
  image_cache_expiry_days: number | null;
  enable_registrations: boolean | null;
  registration_max_allowed_users: number | null;
  setup_completed: boolean;
  tvdb_subscriber_pin: string | null;
  radarr_customize_on_add: boolean;
  sonarr_customize_on_add: boolean;
  disable_comments: boolean;
  disable_user_ratings: boolean;
  enable_logged_out_navigation: boolean;
}

export interface MediaRequestItem {
  id: number;
  uri_id: string;
  media_type: string;
  title: string;
  poster_path: string | null;
  status: "pending" | "approved" | "rejected";
  season_number: number | null;
  episode_number: number | null;
  show_uri_id?: string | null;
  reviewed_by: number | null;
  created_at: string;
  updated_at: string;
  user: { id: number; username: string; display_name: string };
}

export interface LoginResponse {
  access_token: string | null;
  token_type: string;
  requires_2fa: boolean;
  temp_token: string | null;
}

export interface TotpSetupData {
  provisioning_uri: string;
  secret: string;
}

export interface DevicePending {
  client_name: string;
  scope: string;
  requested_at: string;
}

export interface DeviceGrant {
  id: number;
  client_name: string;
  scope: string;
  created_at: string;
  approved_at: string | null;
  last_seen_at: string | null;
}

export interface TotpBackupCode {
  id: number;
  code: string;
  used: boolean;
}

export interface TotpBackupCodesResponse {
  codes: TotpBackupCode[];
}

export interface OidcConfig {
  enabled: boolean;
  provider_name: string;
  disable_password_login: boolean;
}

export interface OidcAuthorizeResponse {
  auth_url: string;
  state: string;
}

export interface OidcExchangeResponse {
  access_token: string;
}

export interface PlexAuthorizeResponse {
  auth_url: string;
  pin_id: string;
}

export interface PlexExchangeResponse {
  access_token: string;
}

export interface PlexServerOption {
  name: string;
  client_identifier: string;
  url: string;
  owned: boolean;
  already_added: boolean;
}

export interface PlexLinkResponse {
  status: string;
  plex_username: string;
  servers: PlexServerOption[];
}

export type PrivacyLevel = "public" | "friends_only" | "private";

export type VisibilityAction = "show" | "fade" | "hide";

export interface ExploreFilters {
  watched: VisibilityAction;
  unwatched: VisibilityAction;
  collected: VisibilityAction;
  uncollected: VisibilityAction;
  listed: VisibilityAction;
  unlisted: VisibilityAction;
}

export interface UserPreferences {
  display_name: string | null;
  bio: string | null;
  country: string | null;
  liked_genres: string[];
  disliked_genres: string[];
  streaming_services: string[];
  content_language: string | null;
  metadata_language: string | null;
  privacy_level: PrivacyLevel;
  avatar_url: string | null;
  pagination_type?: "infinite_scroll" | "pagination";
  explore_filters?: ExploreFilters;
  unified_filters?: boolean;
  show_bio?: boolean;
  specials_label?: boolean;
  primary_metadata_source?: "tmdb" | "tvdb";
  media_server_enrichment?: boolean;
  include_specials?: boolean;
}

export interface UserSettings {
  tmdb_api_key: string | null;
  has_effective_tmdb_key: boolean;
  has_global_tmdb_key: boolean;

  tvdb_api_key: string | null;
  has_global_tvdb_key: boolean;
  has_effective_tvdb_key: boolean;
  default_episode_order?: string | null;

  radarr_url: string | null;
  radarr_token: string | null;
  radarr_root_folder: string | null;
  radarr_quality_profile: number | null;
  radarr_tags: number[] | null;
  has_global_radarr_config: boolean;

  sonarr_url: string | null;
  sonarr_token: string | null;
  sonarr_root_folder: string | null;
  sonarr_quality_profile: number | null;
  sonarr_tags: number[] | null;
  has_global_sonarr_config: boolean;

  // Trakt
  trakt_client_id: string | null;
  trakt_client_secret: string | null;
  trakt_connected: boolean;
  trakt_sync_watched: boolean;
  trakt_sync_ratings: boolean;
  trakt_sync_lists: boolean;
  trakt_watchlist_split: boolean;
  trakt_push_watched: boolean;
  trakt_push_ratings: boolean;
  trakt_push_lists: boolean;
  trakt_scrobble: boolean;

  // Simkl
  simkl_client_id: string | null;
  simkl_connected: boolean;
  simkl_sync_watched: boolean;
  simkl_sync_ratings: boolean;
  simkl_sync_lists: boolean;
  simkl_push_watched: boolean;
  simkl_push_ratings: boolean;
  simkl_scrobble: boolean;

  // MDBList
  mdblist_api_key: string | null;
  mdblist_connected: boolean;
  mdblist_sync_watched: boolean;
  mdblist_sync_ratings: boolean;
  mdblist_sync_watchlist: boolean;
  mdblist_push_watched: boolean;
  mdblist_push_ratings: boolean;
  mdblist_push_watchlist: boolean;

  preferences: UserPreferences | null;
  blur_explicit: boolean;
  blur_unwatched_episode_images: boolean;
  blur_unwatched_episode_overviews: boolean;
  blur_unwatched_movie_images: boolean;
  blur_unwatched_movie_overviews: boolean;
  show_comments: boolean;
  show_user_ratings: boolean;
  time_format_24h: boolean;
  timezone: string | null;
  use_hls_player: boolean;
  playback_target: "web" | "internal";
  onboarded?: boolean;
  shuffle_next_up: boolean;
  minimalist_next_up: boolean;
  rate_prompt_movies: boolean;
  rate_prompt_episodes: boolean;

  radarr_customize_on_add?: boolean;
  sonarr_customize_on_add?: boolean;
  sonarr_season_folder?: boolean | null;
  has_effective_radarr?: boolean;
  has_effective_sonarr?: boolean;

  trakt_full_sync_interval?: number | null;
  trakt_partial_sync_interval?: number | null;
  trakt_sync_dropped?: boolean | null;
  mdblist_scrobble?: boolean | null;

  bingebase_api_key?: string | null;
  bingebase_webhook_url?: string | null;
  bingebase_scrobble?: boolean | null;
  bingebase_push_watched?: boolean | null;
  bingebase_connected?: boolean | null;
}

export interface MediaServerConnection {
  id: number;
  user_id: number;
  type: "jellyfin" | "emby" | "plex" | "nuvio" | "stremio" | "arvio";
  name: string;
  url: string;
  token: string;
  server_user_id: string | null;
  server_username: string | null;
  external_server_url: string | null;
  watchlist_all_users: boolean;
  sync_collection: boolean;
  sync_watched: boolean;
  sync_ratings: boolean;
  sync_playback: boolean;
  push_watched: boolean;
  push_collection: boolean;
  push_playback: boolean;
  push_ratings: boolean;
  auto_sync_interval: number | null;
  partial_sync_interval: number | null;
  auto_push_interval: number | null;
  watchlist_monitored_users: string[] | null;
  watchlist_to_radarr: boolean;
  watchlist_to_sonarr: boolean;
  plex_sync_watchlist: boolean;
  plex_push_watchlist: boolean;
  plex_account_id: string | null;
  plex_machine_identifier: string | null;
  created_at: string;
}

export interface MediaServerConnectionCreate {
  type: "jellyfin" | "emby" | "plex" | "nuvio" | "stremio" | "arvio";
  name: string;
  url: string;
  token: string;
  server_user_id?: string | null;
  server_username?: string | null;
  external_server_url?: string | null;
  watchlist_all_users?: boolean;
  sync_collection?: boolean;
  sync_watched?: boolean;
  sync_ratings?: boolean;
  sync_playback?: boolean;
  push_watched?: boolean;
  push_collection?: boolean;
  push_playback?: boolean;
  push_ratings?: boolean;
  auto_sync_interval?: number | null;
  auto_push_interval?: number | null;
  plex_auth_token?: string | null;
  plex_account_id?: string | null;
  plex_machine_identifier?: string | null;
}

export type MediaServerConnectionUpdate = Partial<Omit<MediaServerConnectionCreate, "type">>;

export interface PlexPinStart {
  pin_id: number;
  auth_url: string;
  interval: number;
}

export interface PlexLoginConnection {
  uri: string;
  local: boolean;
  relay: boolean;
  protocol: string | null;
  reachable: boolean;
  label: string;
}

export interface PlexLoginServer {
  name: string;
  machine_identifier: string;
  owned: boolean;
  url: string;
  token: string;
  connections: PlexLoginConnection[];
}

export type PlexPinPoll =
  | { status: "pending" }
  | {
      status: "connected";
      account: { id: string; username: string; title: string; email: string };
      auth_token: string;
      servers: PlexLoginServer[];
    };

export interface ScrobbleConnection {
  id: number;
  user_id: number;
  type: "jellyfin" | "emby" | "plex";
  name: string;
  server_user_id: string | null;
  server_username: string | null;
  sync_collection: boolean;
  sync_watched: boolean;
  sync_playback: boolean;
  created_at: string;
}

export interface ScrobbleConnectionCreate {
  type: "jellyfin" | "emby" | "plex";
  name: string;
  server_user_id?: string | null;
  server_username?: string | null;
  sync_collection?: boolean;
  sync_watched?: boolean;
  sync_playback?: boolean;
}

export type ScrobbleConnectionUpdate = Pick<ScrobbleConnectionCreate, "sync_collection" | "sync_watched" | "sync_playback">;

export interface ServiceStatus {
  configured: boolean;
  connected: boolean;
  quality_profiles?: { id: number; name: string }[];
  root_folders?: { path: string; freeSpace: number }[];
  tags?: { id: number; label: string }[];
}

export interface ConnectionStatus {
  radarr: ServiceStatus;
  sonarr: ServiceStatus;
  trakt: ServiceStatus;
  simkl: ServiceStatus;
  mdblist: ServiceStatus;
}

export interface TvdbEpisode {
  tvdb_id: number | null;
  tmdb_id: number | null;
  show_tmdb_id: number | null;
  tmdb_season_number: number;
  tmdb_episode_number: number;
  unmatched: boolean;
  season_number: number;
  episode_number: number;
  name: string | null;
  overview: string | null;
  air_date: string | null;
  runtime: number | null;
  image_url: string | null;
  id: number | null;
  in_library: boolean;
  watched: boolean;
  user_rating: number | null;
  in_lists: number[];
}

export interface TvdbEpisodeDetail extends TvdbEpisode {
  play_count: number;
  library: {
    resolution: string | null;
    video_codec: string | null;
    audio_codec: string | null;
    audio_channels: string | null;
    audio_languages: string[] | null;
    subtitle_languages: string[] | null;
  } | null;
  cast: { tmdb_id: null; person_id: number | null; name: string; character: string; profile_path: string | null }[];
  episodes: { episode_number: number; name: string | null }[];
  show: { id: number | null; tvdb_id: number; tmdb_id: number | null; episode_order: "tvdb"; title: string; poster_path: string | null; backdrop_path: string | null };
  season: { name: string; season_number: number; poster_path: string | null };
}

export interface TvdbSeason {
  tvdb_id: number;
  season_number: number;
  name: string;
  overview: string | null;
  tmdb_rating: number | null;
  poster_path: string | null;
  backdrop_path: string | null;
  air_date: string | null;
  episodes: TvdbEpisode[];
  season_in_library: boolean;
  season_watch_pct: number;
  season_watch_started?: boolean;
  season_watched: boolean;
  season_collection_pct: number;
  season_user_rating: number | null;
  show_in_library: boolean;
  show: {
    id: number | null;
    tvdb_id: number;
    tmdb_id: number | null;
    episode_order: "tvdb";
    title: string;
    poster_path: string | null;
    backdrop_path: string | null;
    seasons_meta: TvdbSeasonMeta[];
  };
}

export interface TvdbSeasonMeta {
  season_number: number;
  name: string;
  overview: string | null;
  tmdb_rating: number | null;
  poster_path: string | null;
  episode_count: number;
  air_date: string | null;
}

export interface ShowRewatch {
  started_at: string;
  watched: number;
  total: number;
}

export interface TvdbShow {
  id: number | null;
  tvdb_id: number;
  tmdb_id: number | null;
  type: string;
  title: string;
  original_title: string | null;
  overview: string | null;
  poster_path: string | null;
  backdrop_path: string | null;
  first_air_date: string | null;
  last_air_date: string | null;
  status: string | null;
  tagline: null;
  tmdb_rating: number | null;
  age_rating: string | null;
  original_language: string | null;
  imdb_id: string | null;
  tmdb_id_cross: number | null;
  episode_order: "tvdb";
  genres: string[];
  network: string | null;
  networks: { id: null; name: string; logo_path: null; origin_country: null }[];
  seasons: TvdbSeasonMeta[];
  seasons_meta: TvdbSeasonMeta[];
  cast: { tmdb_id: null; person_id: number | null; name: string; character: string; profile_path: string | null }[];
  in_library: boolean;
  watched: boolean;
  watch_pct?: number;
  watch_started?: boolean;
  dropped: boolean;
  in_lists: number[];
  collection_pct: number;
  is_monitored: boolean;
  request_enabled: boolean;
  request_status: string | null;
  user_rating: number | null;
  season_states: Record<number, SeasonState>;
  where_to_watch: { type: string; name: string; logo: string | null }[];
  rewatch?: ShowRewatch | null;
}

export interface MediaItem {
  id: number | null;
  remapped_from?: RemappedFrom | null;
  tmdb_id: number | null;
  tvdb_id?: number | null;
  uri_id?: string | null;
  type: MediaType;
  title: string;
  original_title?: string | null;
  overview?: string | null;
  poster_path: string | null;
  backdrop_path?: string | null;
  release_date?: string | null;
  first_air_date?: string | null;
  tmdb_rating?: number | null;
  season_number?: number | null;
  season_name?: string | null;
  episode_number?: number | null;
  runtime?: number | null;
  genres?: string[];
  cast?: CastMember[];
  tagline?: string | null;
  status?: string | null;
  original_language?: string | null;
  age_rating?: string | null;
  imdb_id?: string | null;
  adult?: boolean;
  // #319 - TMDB tags these via community-added keywords rather than a
  // dedicated field, so they're derived server-side from the keyword list.
  // Movies and shows both carry them.
  has_mid_credits_scene?: boolean;
  has_post_credits_scene?: boolean;
  show_id?: number | null;
  show_title?: string | null;
  show_tmdb_id?: number | null;
  show_tvdb_id?: number | null;
  show_uri_id?: string | null;
  show_poster_path?: string | null;
  show_backdrop_path?: string | null;
  // True when this episode has no real TMDB counterpart and was enriched
  // from TVDB instead (see #101) — its season/episode numbers are TVDB's
  // raw numbers, not TMDB's, regardless of whether show_tmdb_id is set.
  tvdb_sourced?: boolean;
  // The show's actual TVDB/TMDB numbering preference (#186) and, when it's
  // "tvdb", this item's translated position — see lib/episodeHref.ts, which
  // every card/link builder should go through rather than re-deriving this.
  show_episode_order?: "tmdb" | "tvdb" | null;
  tvdb_season_number?: number | null;
  tvdb_episode_number?: number | null;
  next_up_hidden?: boolean;
  // Next Up remaining-content estimate (#170) — released unwatched episodes
  // for the show and their estimated total runtime in minutes.
  episodes_left?: number | null;
  remaining_runtime?: number | null;
  known_for_department?: string | null;
  in_library?: boolean;
  playable?: boolean;
  // Card action state
  watched?: boolean;
  in_lists?: number[];
  collection_pct?: number;
  watch_pct?: number;
  watch_started?: boolean;
  is_monitored?: boolean;
  request_enabled?: boolean;
  is_blocked?: boolean;
  is_dropped?: boolean;
  user_rating?: number | null;
  play_count?: number;
  library?: {
    resolution: string;
    video_codec: string;
    audio_codec: string;
    audio_channels: string;
    audio_languages: string[];
    subtitle_languages: string[];
  } | null;
  where_to_watch?: { type: string; name: string; logo: string | null; is_subscribed?: boolean; category?: string; url?: string | null; connection_id?: number | null }[];
  collection?: {
    id: number;
    name: string;
    poster_path: string | null;
    backdrop_path: string | null;
    parts: MediaItem[];
  };
  production_companies?: ProductionCompany[];
  recommendations?: MediaItem[];
  // Progress/Stats
  watched_episodes_count?: number;
  total_episodes_count?: number;
  total_plays?: number;
  total_duration_watched?: number;
  remaining_episodes_count?: number;
  remaining_duration?: number;
  progress_percent?: number;
  trailer_youtube_id?: string | null;
  logo_path?: string | null;
  release_dates?: { digital?: string | null; physical?: string | null } | null;
  still_path?: string | null;
  request_status?: string | null;
  directors?: { tmdb_id: number | null; name: string }[];
}

export interface SubtitleTrack {
  index: number;
  language: string | null;
  label: string | null;
  codec: string | null;
}

export interface PlaybackSource {
  connection_id: number;
  source: string;
  name: string;
  resolution: string | null;
  subtitles: SubtitleTrack[];
}

export interface NowPlayingMedia {
  id: number;
  tmdb_id: number | null;
  uri_id?: string | null;
  type: MediaType;
  title: string;
  poster_path: string | null;
  backdrop_path: string | null;
  season_number: number | null;
  episode_number: number | null;
  runtime: number | null;
  show_title?: string;
  show_tmdb_id?: number | null;
  show_uri_id?: string | null;
  show_tvdb_id?: number | null;
  show_poster_path?: string | null;
}

export interface NowPlayingSession {
  session_key: string;
  source: string;
  state: "playing" | "paused";
  progress_percent: number;
  progress_seconds: number;
  started_at: string;
  updated_at: string;
  media: NowPlayingMedia;
}

export interface ContinueWatchingItem {
  id: number;
  media: MediaItem;
  user_id: number;
  watched_at: string;
  progress_seconds: number | null;
  progress_percent: number | null;
  completed: boolean;
}

export interface ShowProgress {
  id: number;
  tmdb_id: number | null;
  tvdb_id: number | null;
  uri_id: string | null;
  type: "series";
  title: string;
  poster_path: string | null;
  status: string | null;
  genres: string[];
  watched_episodes: number;
  total_episodes: number;
  remaining_episodes: number;
  watch_pct: number;
  watched_minutes: number;
  replay_minutes: number;
  last_watched_at: string | null;
  episodes: { season: number; episode: number; watched: boolean }[];
}

export interface ProgressTotals {
  watched_minutes: number;
  replay_minutes: number;
  watched_episodes: number;
}

export interface CollectionDetail {
  id: number;
  name: string;
  overview: string | null;
  poster_path: string | null;
  backdrop_path: string | null;
  genres: string[];
  cast: { tmdb_id: number | null; uri_id?: string | null; name: string; profile_path: string | null; appearances: number }[];
  parts: MediaItem[];
}

export interface Show {
  id: number | null;
  tmdb_id: number | null;          // null for TVDB-only shows
  tvdb_id?: number | null;
  tmdb_id_cross?: number | null;   // TVDB shows that cross-reference TMDB
  uri_id?: string | null;
  episode_order?: "tmdb" | "tvdb";
  title: string;
  original_title: string | null;
  overview: string;
  poster_path: string | null;
  backdrop_path: string | null;
  tmdb_rating: number | null;      // null for TVDB shows
  first_air_date: string | null;
  genres: string[];
  in_library: boolean;
  watched: boolean;
  watch_pct?: number;
  watch_started?: boolean;
  in_lists: number[];
  collection_pct: number;
  is_monitored?: boolean;
  request_enabled?: boolean;
  seasons: Record<string, MediaItem[]>;
  seasons_meta: SeasonMeta[];
  season_states: Record<number, SeasonState>;
  cast: CastMember[];
  networks: Network[];
  recommendations: MediaItem[];
  tagline: string | null;
  status: string | null;
  original_language?: string | null;
  age_rating?: string | null;
  imdb_id?: string | null;
  adult?: boolean;
  // #319 - derived server-side from TMDB's community keywords, same as movies.
  has_mid_credits_scene?: boolean;
  has_post_credits_scene?: boolean;
  is_blocked?: boolean;
  is_dropped?: boolean;
  user_rating?: number | null;
  last_air_date: string | null;
  where_to_watch?: { type: string; name: string; logo: string | null; is_subscribed?: boolean; category?: string; url?: string | null; connection_id?: number | null }[];
  trailer_youtube_id?: string | null;
  logo_path?: string | null;
  include_specials?: boolean;
  watched_episodes_count?: number;
  total_episodes_count?: number;
  custom_title?: string | null;
  request_status?: string | null;
  created_by?: { tmdb_id: number | null; name: string }[];
}

export interface ProfileWatchedItem {
  tmdb_id: number | null;
  uri_id?: string | null;
  media_type: string;
  title: string;
  poster_path: string | null;
  backdrop_path: string | null;
  watched_at: string;
  show_title: string | null;
  show_tmdb_id: number | null;
  show_tvdb_id?: number | null;
  show_uri_id?: string | null;
  show_poster_path: string | null;
  season_number: number | null;
  episode_number: number | null;
}

export interface ProfileRatedItem {
  tmdb_id: number | null;
  uri_id?: string | null;
  media_type: string;
  title: string;
  poster_path: string | null;
  backdrop_path: string | null;
  user_rating: number;
}

export interface ContentFilters {
  blocked_genres: string[];
  blocked_keywords: string[];
  blocked_regexes: string[];
  filter_languages: string[];
  language_filter_mode: "blacklist" | "whitelist";
  blocked_ratings: string[];
  available_genres: string[];
  available_ratings: string[];
}

export interface UserSearchResult {
  id: number;
  username: string;
  display_name: string;
  avatar_url: string | null;
  pagination_type?: "infinite_scroll" | "pagination";
  country: string | null;
  movies_watched: number;
  shows_watched: number;
  total_collected: number;
  total_rated: number;
  follower_count: number;
  is_following: boolean;
  is_self: boolean;
}

export interface ProfileFollowEntry {
  id: number;
  display_name: string;
  avatar_url: string | null;
  pagination_type?: "infinite_scroll" | "pagination";
}

export interface ProfileListItem {
  id: number;
  name: string;
  description: string | null;
  privacy_level: PrivacyLevel;
  item_count: number;
  updated_at: string;
  preview_posters: { url: string; adult: boolean }[];
}

export interface ProfileCommentItem {
  id: number;
  content: string;
  media_type: string;
  tmdb_id: number | null;
  uri_id?: string | null;
  season_number: number | null;
  season_name: string | null;
  episode_number: number | null;
  title: string | null;
  poster_path: string | null;
  created_at: string;
}

export interface PublicProfile {
  id: number;
  username: string;
  display_name: string;
  bio: string | null;
  country: string | null;
  liked_genres: string[];
  created_at: string;
  total_watched: number;
  total_collected: number;
  movies_watched: number;
  shows_watched: number;
  total_rated: number;
  avatar_url: string | null;
  pagination_type?: "infinite_scroll" | "pagination";
  recently_watched_movies: ProfileWatchedItem[];
  recently_watched_shows: ProfileWatchedItem[];
  top_rated_movies: ProfileRatedItem[];
  top_rated_shows: ProfileRatedItem[];
  recent_comments: ProfileCommentItem[];
  lists: ProfileListItem[];
  follower_count: number;
  following_count: number;
  followers: ProfileFollowEntry[];
  following: ProfileFollowEntry[];
  is_following: boolean;
}

export interface UserStats {
  movies_watched: number;
  shows_watched: number;
  episodes_watched: number;
  total_watch_minutes: number;
  watch_activity: { month: string; movies: number; episodes: number }[];
  rating_distribution: { rating: number; count: number }[];
  avg_movie_rating: number | null;
  avg_show_rating: number | null;
  movies_collected: number;
  shows_collected: number;
  episodes_collected: number;
  movies_watched_collected: number;
  movies_unwatched_collected: number;
  shows_watched_collected: number;
  shows_unwatched_collected: number;
  weekday_activity: { day: string; avg: number }[];
}

export interface Comment {
  id: number;
  user_id: number;
  username: string;
  display_name: string;
  avatar_url: string | null;
  pagination_type?: "infinite_scroll" | "pagination";
  user_is_public: boolean;
  content: string;
  is_spoiler: boolean;
  created_at: string;
  updated_at?: string | null;
}

// API calls
export interface PublicAccessStatus {
  enable_logged_out_navigation: boolean;
  disable_comments: boolean;
  disable_user_ratings: boolean;
}

const PUBLIC_ACCESS_STATUS_TTL_MS = 5000;
const PUBLIC_ACCESS_STATUS_FALLBACK: PublicAccessStatus = {
  enable_logged_out_navigation: false,
  disable_comments: false,
  disable_user_ratings: false,
};
let publicAccessStatusCache: { at: number; promise: Promise<PublicAccessStatus> } | null = null;

export const api = {
  auth: {
    login: (body: FormData) =>
      post<LoginResponse>("/auth/login", body),
    register: (body: unknown) =>
      post<{ id: number; email: string; username: string; email_confirmed?: boolean }>("/auth/register", body),
    registrationStatus: () =>
      get<{ enabled: boolean; smtp_configured: boolean }>("/auth/registration-status"),
    hasUsers: () =>
      get<{ has_users: boolean }>("/auth/has-users"),
    activateEmail: (token: string) =>
      post<{ success: boolean }>(`/auth/activate/${token}`, undefined),
    forgotPassword: (email: string) =>
      post<{ message: string }>("/auth/forgot-password", { email }),
    resetPassword: (token: string, new_password: string) =>
      post<{ message: string }>(`/auth/reset-password/${token}`, { new_password }),
    me: (token: string) =>
      get<UserProfile>("/auth/me", undefined, token),
    devicePending: (userCode: string, token: string) =>
      get<DevicePending>("/auth/device/pending", { user_code: userCode }, token),
    deviceGrants: (token: string) =>
      get<DeviceGrant[]>("/auth/device/grants", undefined, token),
    getSettings: (token: string) =>
      get<UserSettings>("/auth/settings", undefined, token),
    updateSettings: (settings: Partial<UserSettings>, token: string) =>
      patch<UserSettings>("/auth/settings", settings, token),
    changePassword: (body: unknown, token: string) =>
      post<{ message: string }>("/auth/change-password", body, token),
    deleteAccount: (token: string) =>
      del<{ message: string }>("/auth/me", undefined, token),
    regenerateApiKey: (token: string) =>
      post<UserProfile>("/auth/api-key/regenerate", undefined, token),
    getConnections: (token: string) =>
      get<MediaServerConnection[]>("/auth/connections", undefined, token),
    createConnection: (body: MediaServerConnectionCreate, token: string) =>
      post<MediaServerConnection>("/auth/connections", body, token),
    updateConnection: (id: number, body: MediaServerConnectionUpdate, token: string) =>
      patch<MediaServerConnection>(`/auth/connections/${id}`, body, token),
    deleteConnection: (id: number, token: string) =>
      del<{ status: string }>(`/auth/connections/${id}`, undefined, token),
    startStremioLink: (token: string) =>
      post<{ code: string; link: string; qrcode: string }>("/auth/stremio/link/start", undefined, token),
    pollStremioLink: (body: { code: string; name: string }, token: string) =>
      post<{ status: "pending" | "connected"; connection?: MediaServerConnection }>("/auth/stremio/link/poll", body, token),
    getScrobbleConnections: (token: string) =>
      get<ScrobbleConnection[]>("/auth/scrobble-connections", undefined, token),
    createScrobbleConnection: (body: ScrobbleConnectionCreate, token: string) =>
      post<ScrobbleConnection>("/auth/scrobble-connections", body, token),
    updateScrobbleConnection: (id: number, body: ScrobbleConnectionUpdate, token: string) =>
      patch<ScrobbleConnection>(`/auth/scrobble-connections/${id}`, body, token),
    deleteScrobbleConnection: (id: number, token: string) =>
      del<{ status: string }>(`/auth/scrobble-connections/${id}`, undefined, token),
    testJellyfin: (url: string, token: string, jellyfinUserId: string | null, userToken: string) =>
      post<{ success: boolean; message: string }>("/auth/test-jellyfin", { url, token, user_id: jellyfinUserId }, userToken),
    testEmby: (url: string, token: string, embyUserId: string | null, userToken: string) =>
      post<{ success: boolean; message: string }>("/auth/test-emby", { url, token, user_id: embyUserId }, userToken),
    testPlex: (url: string, token: string, userToken: string) =>
      post<{ success: boolean; message: string }>("/auth/test-plex", { url, token }, userToken),
    testRadarr: (url: string, token: string, userToken: string) =>
      post<{ success: boolean; message: string }>("/auth/test-radarr", { url, token }, userToken),
    getRadarrProfiles: (url: string, token: string, userToken: string) =>
      post<{ quality_profiles: any[]; root_folders: any[] }>("/auth/radarr/profiles", { url, token }, userToken),
    testSonarr: (url: string, token: string, userToken: string) =>
      post<{ success: boolean; message: string }>("/auth/test-sonarr", { url, token }, userToken),
    getSonarrProfiles: (url: string, token: string, userToken: string) =>
      post<{ quality_profiles: any[]; root_folders: any[]; language_profiles: any[] }>("/auth/sonarr/profiles", { url, token }, userToken),
    testTmdb: (key: string, userToken: string) =>
      post<{ success: boolean; message: string }>("/auth/test-tmdb", { key }, userToken),
    getConnectionStatus: (token: string) =>
      get<ConnectionStatus>("/auth/connection-status", undefined, token),
    totp2faSetup: (token: string) =>
      post<TotpSetupData>("/auth/2fa/setup", undefined, token),
    totp2faEnable: (body: { secret: string; code: string }, token: string) =>
      post<TotpBackupCodesResponse>("/auth/2fa/enable", body, token),
    totp2faDisable: (body: { code: string }, token: string) =>
      post<{ status: string }>("/auth/2fa/disable", body, token),
    totp2faBackupCodes: (token: string) =>
      get<TotpBackupCodesResponse>("/auth/2fa/backup-codes", undefined, token),
    totp2faVerifyLogin: (body: { temp_token: string; code: string }) =>
      post<LoginResponse>("/auth/2fa/verify-login", body),
    oidcConfig: () =>
      get<OidcConfig>("/auth/oidc/config"),
    oidcAuthorize: () =>
      get<OidcAuthorizeResponse>("/auth/oidc/authorize"),
    oidcExchange: (code: string) =>
      post<OidcExchangeResponse>("/auth/oidc/exchange", { code }),
    plexAuthorize: (link = false, redirectUri?: string) =>
      get<PlexAuthorizeResponse>(
        `/auth/plex/authorize?link=${link}${redirectUri ? `&redirect_uri=${encodeURIComponent(redirectUri)}` : ""}`,
      ),
    plexExchange: (pin_id: string) =>
      post<PlexExchangeResponse>("/auth/plex/exchange", { pin_id }),
    plexLink: (pin_id: string, token: string) =>
      post<PlexLinkResponse>("/auth/plex/link", { pin_id }, token),
    plexCreateConnections: (pin_id: string, client_identifiers: string[], token: string) =>
      post<{ added: number }>("/auth/plex/connections", { pin_id, client_identifiers }, token),
    plexUnlink: (token: string) =>
      post<{ status: string }>("/auth/plex/unlink", undefined, token),
  },

  trakt: {
    deviceStart: (token: string) =>
      post<{ user_code: string; verification_url: string; expires_in: number; interval: number }>("/trakt/auth/device/start", undefined, token),
    devicePoll: (token: string) =>
      post<{ status: "pending" | "connected" }>("/trakt/auth/device/poll", undefined, token),
    disconnect: (token: string) =>
      del<{ status: string }>("/trakt/auth/disconnect", undefined, token),
    sync: (token: string) =>
      post<{ status: string; job_id: number; message: string }>("/trakt/sync", undefined, token),
  },

  simkl: {
    pinStart: (token: string) =>
      post<{ user_code: string; url: string; expires_in: number; interval: number }>("/simkl/auth/pin/start", undefined, token),
    pinPoll: (token: string) =>
      post<{ status: "pending" | "connected" }>("/simkl/auth/pin/poll", undefined, token),
    disconnect: (token: string) =>
      del<{ status: string }>("/simkl/auth/disconnect", undefined, token),
    sync: (token: string) =>
      post<{ status: string; job_id: number; message: string }>("/simkl/sync", undefined, token),
    push: (token: string) =>
      post<{ status: string; message: string }>("/simkl/push", undefined, token),
  },

  mdblist: {
    sync: (token: string) =>
      post<{ status: string; job_id: number; message: string }>("/mdblist/sync", undefined, token),
    push: (token: string) =>
      post<{ status: string; job_id: number; message: string }>("/mdblist/push", undefined, token),
  },

  media: {
    list: (params?: { type?: string; sort?: string; page?: number; genre?: string[]; year?: number[]; watched?: string[]; in_list?: string }, token?: string) =>
      get<{ page: number; page_size: number; total_pages: number; total_results: number; results: MediaItem[] }>("/media", params, token),

    years: (type: string, token?: string) =>
      get<{ years: number[] }>("/media/years", { type }, token),

    get: (type: string, idOrUri: number | string, token?: string) =>
      get<MediaItem>(`/media/${type}/${idOrUri}`, undefined, token),

    getRecommendations: (type: string, idOrUri: number | string, token?: string) =>
      get<{ results: MediaItem[] }>(`/media/${type}/${idOrUri}/recommendations`, undefined, token),

    getPerson: (
      personId: number | string,
      page: number = 1,
      token?: string,
      filters?: { collection?: "in" | "out" | ""; genre?: string[]; year?: number[]; minRating?: string },
    ) =>
      get<PersonDetail>(`/media/person/${personId}`, {
        page,
        collection: filters?.collection || undefined,
        genre: filters?.genre?.length ? filters.genre : undefined,
        year: filters?.year?.length ? filters.year : undefined,
        min_rating: filters?.minRating || undefined,
      }, token),

    getCollection: (collectionId: number, token?: string) =>
      get<CollectionDetail>(`/media/collection/${collectionId}`, undefined, token),

    tmdbList: (params: { type: string; category?: string; page?: number; genre?: string[]; year?: number[]; min_rating?: number; status?: string; provider_id?: number; original_language?: string; collection?: string[]; watch?: string[]; arr?: string[]; in_list?: string[] }, token?: string) =>
      get<{ results: MediaItem[]; page: number; total_pages: number; total_results: number }>("/media/tmdb/list", params, token),

    getBlocklist: (token?: string) =>
      get<{ uri_id: string; tmdb_id: number | null; media_type: string; is_dropped: boolean }[]>("/media/blocklist", undefined, token),

    getBlocklistEnriched: (token?: string) =>
      get<MediaItem[]>("/media/blocklist/enriched", undefined, token),

    block: (uriId: string, mediaType: string, token?: string) =>
      post<{ status: string }>("/media/blocklist", { uri_id: uriId, media_type: mediaType, is_dropped: false }, token),

    drop: (uriId: string, mediaType: string, token?: string) =>
      post<{ status: string }>("/media/blocklist", { uri_id: uriId, media_type: mediaType, is_dropped: true }, token),

    unblock: (uriId: string, mediaType: string, token?: string) =>
      del<{ status: string }>("/media/blocklist", { uri_id: uriId, media_type: mediaType }, token),

    search: (q: string, type?: string, page: number = 1, year?: number, token?: string, inLibrary?: boolean) =>
      get<{ results: MediaItem[]; page: number; total_pages: number; total_results: number }>("/media/search", { q, ...(type ? { type } : {}), page, ...(year ? { year } : {}), ...(inLibrary ? { in_library: true } : {}) }, token),

    searchTvdb: (q: string, token?: string) =>
      get<{ tvdb_id: number; title: string; overview: string | null; year: string | null; image_url: string | null; status: string | null; network: string | null }[]>("/media/search-tvdb", { q }, token),

    recentlyAdded: (type?: string, token?: string) =>
      get<{ results: MediaItem[] }>("/media/recently-added", type ? { type } : {}, token),

    onAirToday: (page: number = 1, token?: string, timezone?: string) =>
      get<{ results: MediaItem[]; page: number; total_pages: number; total_results: number }>("/media/on-air-today", { page, ...(timezone ? { timezone } : {}) }, token),

    airingTodayCollected: (page: number = 1, token?: string, timezone?: string) =>
      get<{ results: MediaItem[]; page: number; total_pages: number; total_results: number }>("/media/airing-today/collected", { page, ...(timezone ? { timezone } : {}) }, token),

    publicPosterWall: () =>
      get<{ posters: { poster_path: string; title: string | null; media_type: "movie" | "series" }[] }>("/media/public/poster-wall"),

    trendingMovies: (page: number = 1, token?: string) =>
      get<{ results: MediaItem[]; page: number; total_pages: number; total_results: number }>("/media/trending/movies", { page }, token),

    trendingShows: (page: number = 1, token?: string) =>
      get<{ results: MediaItem[]; page: number; total_pages: number; total_results: number }>("/media/trending/shows", { page }, token),

    nowPlaying: (token?: string) =>
      get<{ results: MediaItem[] }>("/media/now-playing", {}, token),

    upcomingMovies: (token?: string) =>
      get<{ results: MediaItem[] }>("/media/upcoming", {}, token),

    onAirThisWeek: (token?: string) =>
      get<{ results: MediaItem[] }>("/media/on-air-this-week", {}, token),

    hiddenGems: (type: string = "movie", token?: string) =>
      get<{ results: MediaItem[] }>("/media/hidden-gems", { type }, token),

    recommended: (token?: string) =>
      get<{ results: MediaItem[] }>("/media/recommended", {}, token),

    forYou: (token?: string) =>
      get<{ results: MediaItem[] }>("/media/for-you", {}, token),

    collect: (body: { uri_id: string; media_type: string }, token: string) =>
      post<{ status: string; message: string }>("/media/collect", body, token),

    request: (type: string, idOrUri: number | string, token: string) =>
      post<{ status: string; movie?: any; series?: any }>(`/media/${type}/${idOrUri}/request`, undefined, token),

    uncollect: (uriId: string, mediaType: string, token: string) =>
      del<{ status: string }>(`/media/collect?uri_id=${encodeURIComponent(uriId)}&media_type=${mediaType}`, undefined, token),

    refreshMovie: (idOrUri: number | string, token: string) =>
      post<{ message: string }>(`/media/movie/${idOrUri}/refresh`, undefined, token),

    playbackSources: (type: string, mediaRef: string | number, token: string) =>
      get<PlaybackSource[]>(`/media/playback/${type}/${mediaRef}`, undefined, token),

    getWatchProviders: (type: string = "movie", region: string = "US", token?: string) =>
      get<{ results: any[] }>("/media/watch-providers", { type, region }, token),

    getGenres: (type: string = "movie", token?: string) =>
      get<{ genres: { id: number; name: string }[] }>("/media/genres", { type }, token),

    getLanguages: (token?: string) =>
      get<{ iso_639_1: string; english_name: string; name: string }[]>("/media/languages", undefined, token),

    getCountries: (token?: string) =>
      get<{ iso_3166_1: string; english_name: string; native_name: string }[]>("/media/countries", undefined, token),

    updateProgress: (body: { uri_id: string; media_type: string; progress_seconds: number; completed?: boolean }, token: string) =>
      post<{ status: string }>("/media/progress", body, token),
  },

  shows: {
    list: (params?: { sort?: string; page?: number; page_size?: number; genre?: string[]; year?: number[]; status?: string[]; watched?: string[]; in_list?: string }, token?: string) =>
      get<{ page: number; page_size: number; total_results: number; total_pages: number; results: any[] }>("/shows", params, token),

    years: (token?: string) =>
      get<{ years: number[] }>("/shows/years", undefined, token),

    get: (seriesId: number | string, token?: string) =>
      get<Show>(`/shows/${seriesId}`, undefined, token),

    getRecommendations: (seriesId: number | string, token?: string) =>
      get<{ results: MediaItem[] }>(`/shows/${seriesId}/recommendations`, undefined, token),

    getSeason: (seriesId: number | string, seasonNumber: number, token?: string) =>
      get<Season>(`/shows/${seriesId}/season/${seasonNumber}`, undefined, token),

    getEpisode: (seriesId: number | string, seasonNumber: number, episodeNumber: number, token?: string) =>
      get<EpisodeDetail>(`/shows/${seriesId}/season/${seasonNumber}/${episodeNumber}`, undefined, token),

    refreshMetadata: (seriesId: number | string, token: string) =>
      post<{ message: string }>(`/shows/${seriesId}/refresh`, undefined, token),

    setEpisodeOrder: (seriesTmdbId: number, episodeOrder: "tmdb" | "tvdb", token: string, forceRefresh = false) =>
      post<{
        episode_order: "tmdb" | "tvdb";
        series_tmdb_id: number;
        tvdb_id: number | null;
        redirect: string;
        mapping: { matched: number; tmdb_episodes: number; unmatched: number } | null;
      }>(
        `/shows/${seriesTmdbId}/episode-order`,
        { episode_order: episodeOrder, force_refresh: forceRefresh },
        token,
      ),
  
    getTvdb: (tvdbId: number, token?: string) =>
      get<TvdbShow>(`/shows/tvdb/${tvdbId}`, undefined, token),

    getTvdbSeason: (tvdbId: number, seasonNumber: number, token?: string) =>
      get<TvdbSeason>(`/shows/tvdb/${tvdbId}/season/${seasonNumber}`, undefined, token),

    getTvdbEpisode: (tvdbId: number, seasonNumber: number, episodeNumber: number, token?: string) =>
      get<TvdbEpisodeDetail>(`/shows/tvdb/${tvdbId}/season/${seasonNumber}/episode/${episodeNumber}`, undefined, token),
},

  history: {
    list: (params?: { page?: number; page_size?: number; type?: string; start_date?: string; end_date?: string; undated?: boolean }, token?: string) =>
      get<{ page: number; page_size: number; total_pages: number; total_results: number; results: WatchEvent[] }>("/history", params, token),

    markAsWatched: (body: { uri_id?: string; tmdb_id?: number; media_type: string; watched_at?: string | null; completed?: boolean }, token: string) =>
      post<{ message: string }>("/history", body, token),

    unwatchItem: (uriId: string, mediaType: string, token: string) =>
      del<{ status: string }>(`/history/item?uri_id=${encodeURIComponent(uriId)}&media_type=${mediaType}`, undefined, token),

    markSeasonWatched: (body: { show_uri_id: string; season_number: number }, token: string) =>
      post<{ status: string; count: number }>("/history/season", body, token),

    unmarkSeasonWatched: (showUriId: string, seasonNumber: number, token: string) =>
      del<{ status: string }>(`/history/season?show_uri_id=${encodeURIComponent(showUriId)}&season_number=${seasonNumber}`, undefined, token),

    markShowWatched: (body: { show_uri_id: string }, token: string) =>
      post<{ status: string; count: number }>("/history/show-all", body, token),

    unmarkShowWatched: (showUriId: string, token: string) =>
      del<{ status: string }>(`/history/show-all?show_uri_id=${encodeURIComponent(showUriId)}`, undefined, token),

    continueWatching: (params?: { page?: number; page_size?: number } | string, token?: string) => {
      const finalParams = typeof params === "object" ? params : undefined;
      const finalToken = typeof params === "string" ? params : token;
      return get<{ continue_watching: ContinueWatchingItem[]; page: number; total_pages: number; total_results: number }>(
        "/history/continue-watching",
        finalParams as any,
        finalToken
      );
    },

    deleteProgress: (uriId: string, mediaType: string, token: string) =>
      del<{ status: string }>(`/history/continue-watching?uri_id=${encodeURIComponent(uriId)}&media_type=${mediaType}`, undefined, token),

    nextUp: (token?: string, limit?: number, includeHidden?: boolean) =>
      get<{ next_up: MediaItem[] }>("/history/next-up", { ...(limit ? { limit } : {}), ...(includeHidden ? { include_hidden: true } : {}) }, token),

    nowPlaying: (token: string) =>
      get<{ now_playing: NowPlayingSession[] }>("/history/now-playing", undefined, token),

    getItemEvents: (uriId: string, mediaType: string, token: string) =>
      get<{ events: { id: number; watched_at: string; progress_seconds: number | null; progress_percent: number | null; completed: boolean; play_count: number }[] }>(
        `/history/item/events?uri_id=${encodeURIComponent(uriId)}&media_type=${mediaType}`,
        undefined,
        token
      ),

    deleteEvent: (eventId: number, token: string) =>
      del<{ status: string; remaining_count: number }>(`/history/event/${eventId}`, undefined, token),

    progress: (
      params?: { sort?: string; status?: string; genre?: string[]; search?: string; page?: number; page_size?: number },
      token?: string,
    ) =>
      get<{
        results: ShowProgress[];
        page: number;
        total_pages: number;
        total_results: number;
        totals: ProgressTotals;
      }>("/history/progress", params, token),
  },

  lists: {
    getAll: (token: string) =>
      get<{ lists: UserList[] }>("/lists", undefined, token),
    getPublic: (token?: string) =>
      get<{ lists: PublicList[] }>("/lists/public", undefined, token),
    create: (body: Partial<UserList>, token: string) =>
      post<UserList>("/lists", body, token),
    get: (id: number, token?: string) =>
      get<ListDetail>(`/lists/${id}`, undefined, token),
    update: (id: number, body: Partial<UserList>, token: string) =>
      patch<UserList>(`/lists/${id}`, body, token),
    delete: (id: number, token: string) =>
      del<{ message: string }>(`/lists/${id}`, undefined, token),
    addItem: (listId: number, body: { uri_id: string; media_type: string }, token: string) =>
      post<ListItemEntry>(`/lists/${listId}/items`, body, token),
    removeItem: (listId: number, itemId: number, token: string) =>
      del<{ message: string }>(`/lists/${listId}/items/${itemId}`, undefined, token),
  },

  sync: {
    jellyfin: (params?: { movie_limit?: number; show_limit?: number }, token?: string) =>
      post<{ status: string; job_id: number; message: string }>("/sync/jellyfin", params, token),
    emby: (params?: { movie_limit?: number; show_limit?: number }, token?: string) =>
      post<{ status: string; job_id: number; message: string }>("/sync/emby", params, token),
    plex: (params?: { movie_limit?: number; show_limit?: number }, token?: string) =>
      post<{ status: string; job_id: number; message: string }>("/sync/plex", params, token),
    syncConnection: (connectionId: number, params?: { movie_limit?: number; show_limit?: number }, token?: string) =>
      post<{ status: string; job_id: number; message: string }>(`/sync/connection/${connectionId}`, params, token),
    fullResyncConnection: (connectionId: number, token: string) =>
      post<{ status: string; job_id: number; message: string }>(`/sync/connection/${connectionId}?full=true`, undefined, token),
    status: (token: string) =>
      get<SyncJob[]>("/sync/status", undefined, token),
    getConnectionLibraries: (connectionId: number, token: string) =>
      get<{ libraries: { id?: string; key?: string; name: string; type: string; selected: boolean }[]; all_selected: boolean }>(`/sync/connection/${connectionId}/libraries`, undefined, token),
    saveConnectionLibraries: (connectionId: number, body: { library_ids?: string[]; library_keys?: string[] }, token: string) =>
      put<{ saved: number }>(`/sync/connection/${connectionId}/libraries`, body, token),
    scanLibraries: (connectionId: number, token: string) =>
      post<{ status: string; message: string }>(`/sync/connection/${connectionId}/scan`, undefined, token),
    
    // Remaps (Overrides)
    getSeasonOverrides: (token: string) =>
      get<ShowSeasonOverride[]>("/sync/season-overrides", undefined, token),
    createSeasonOverride: (body: any, token: string) =>
      post<ShowSeasonOverride>("/sync/season-overrides", body, token),
    deleteSeasonOverride: (id: number, token: string) =>
      del<{ status: string }>(`/sync/season-overrides/${id}`, undefined, token),
    bulkDeleteSeasonOverrides: (ids: number[], token: string) =>
      post<{ status: string; deleted_count: number }>("/sync/season-overrides/bulk-delete", { ids }, token),
    applySeasonOverride: (id: number, token: string) =>
      post<{ status: string; remapped: number }>(`/sync/season-overrides/${id}/apply`, undefined, token),
    
    getEpisodeOverrides: (token: string) =>
      get<{ overrides: any[] }>("/sync/episode-overrides", undefined, token),
    createEpisodeOverride: (body: any, token: string) =>
      post<{ id: number; status: string }>("/sync/episode-overrides", body, token),
    deleteEpisodeOverride: (id: number, token: string) =>
      del<{ status: string }>(`/sync/episode-overrides/${id}`, undefined, token),
    bulkDeleteEpisodeOverrides: (ids: number[], token: string) =>
      post<{ status: string; deleted_count: number }>("/sync/episode-overrides/bulk-delete", { ids }, token),
    applyEpisodeOverride: (id: number, token: string) =>
      post<{ status: string; remapped: number }>(`/sync/episode-overrides/${id}/apply`, undefined, token),
    applyAllOverrides: (token: string) =>
      post<{ status: string; remapped: number }>("/sync/overrides/apply-all", undefined, token),

    getCustomTitles: (token: string) =>
      get<{ results: any[] }>("/sync/custom-titles", undefined, token),
    saveCustomTitle: (body: any, token: string) =>
      patch<{ status: string }>("/sync/custom-title", body, token),
    deleteCustomTitle: (showUriId: string, token: string) =>
      del<{ status: string }>("/sync/custom-title", { show_uri_id: showUriId }, token),

    getMatchedShows: (token: string) =>
      get<any[]>("/sync/matched-shows", undefined, token),
    getEpisodeMovieConversions: (token: string) =>
      get<{ conversions: any[] }>("/sync/episode-movie-conversions", undefined, token),
    getUnmatchedWarnings: (token: string) =>
      get<{ seasons: any[]; titles: any[] }>("/sync/unmatched-warnings", undefined, token),
    getMatchedMovies: (token: string) =>
      get<any[]>("/sync/matched-movies", undefined, token),
    unmatchShow: (body: { show_title: string }, token: string) =>
      post<{ status: string; unmatched: number }>("/sync/unmatch-show", body, token),
    unmatchMovie: (body: { movie_title: string }, token: string) =>
      post<{ status: string; unmatched: number }>("/sync/unmatch-movie", body, token),

    // Source show/movie lookups for remaps
    getSourceShows: (token: string) =>
      get<any[]>("/sync/source-shows", undefined, token),
    getSourceMovies: (token: string) =>
      get<any[]>("/sync/source-movies", undefined, token),
    getTmdbShowPreview: (uriId: string, token: string) =>
      get<any>(`/sync/tmdb-show-preview/${uriId}`, undefined, token),
  },

  profile: {
    get: (token: string) =>
      get<UserPreferences>("/profile/me", undefined, token),
    getPublic: (userId: number, token?: string) =>
      get<PublicProfile>(`/profile/${userId}`, undefined, token),
    publicAccessStatus: () =>
      get<PublicAccessStatus>("/profile/public-access-status"),
    // Same flags, memoised for a few seconds. ActionBar renders once per card,
    // so gating the rate button on disable_user_ratings through the uncached
    // call above would fan out one request per card on every grid page.
    publicAccessStatusCached: (): Promise<PublicAccessStatus> => {
      const now = Date.now();
      if (!publicAccessStatusCache || now - publicAccessStatusCache.at > PUBLIC_ACCESS_STATUS_TTL_MS) {
        const promise = get<PublicAccessStatus>("/profile/public-access-status").catch(() => {
          publicAccessStatusCache = null;
          return PUBLIC_ACCESS_STATUS_FALLBACK;
        });
        publicAccessStatusCache = { at: now, promise };
      }
      return publicAccessStatusCache.promise;
    },
    update: (body: Partial<UserPreferences>, token: string) =>
      patch<UserPreferences>("/profile/me", body, token),
    uploadAvatar: (formData: FormData, token: string) =>
      request<{ avatar_url: string }>("/profile/me/avatar", "POST", undefined, formData, token),
    deleteAvatar: (token: string) =>
      del<{ status: string }>("/profile/me/avatar", undefined, token),
    follow: (userId: number, token: string) =>
      post<{ status: string }>(`/profile/${userId}/follow`, undefined, token),
    unfollow: (userId: number, token: string) =>
      del<{ status: string }>(`/profile/${userId}/follow`, undefined, token),
    searchUsers: (q: string, token?: string) =>
      get<{ results: UserSearchResult[] }>("/profile/search", { q }, token),
    getStats: (userId: number, token?: string) =>
      get<UserStats>(`/profile/${userId}/stats`, undefined, token),
    topRatedMovies: (userId: number, page: number = 1, token?: string) =>
      get<{ results: ProfileRatedItem[]; page: number; total_pages: number; total_results: number }>(`/profile/${userId}/top-rated-movies`, { page }, token),
    topRatedShows: (userId: number, page: number = 1, token?: string) =>
      get<{ results: ProfileRatedItem[]; page: number; total_pages: number; total_results: number }>(`/profile/${userId}/top-rated-shows`, { page }, token),
    recentlyWatchedMovies: (userId: number, page: number = 1, token?: string) =>
      get<{ results: ProfileWatchedItem[]; page: number; total_pages: number; total_results: number }>(`/profile/${userId}/recently-watched-movies`, { page }, token),
    recentlyWatchedShows: (userId: number, page: number = 1, token?: string) =>
      get<{ results: ProfileWatchedItem[]; page: number; total_pages: number; total_results: number }>(`/profile/${userId}/recently-watched-shows`, { page }, token),
  },

  comments: {
    list: (params: { media_type: string; uri_id: string; season_number?: number; episode_number?: number }, token?: string) =>
      get<Comment[]>("/comments", params, token),
    create: (body: { media_type: string; uri_id: string; season_number?: number; episode_number?: number; content: string }, token: string) =>
      post<Comment>("/comments", body, token),
    update: (id: number, content: string, token: string) =>
      patch<{ id: number; content: string; updated_at: string | null }>(`/comments/${id}`, { content }, token),
    delete: (id: number, token: string) =>
      del<{ message: string }>(`/comments/${id}`, undefined, token),
  },

  admin: {
    getSettings: (token: string) =>
      get<GlobalSettings>("/admin/settings", undefined, token),
    updateSettings: (body: Partial<GlobalSettings>, token: string) =>
      patch<GlobalSettings>("/admin/settings", body, token),
    listUsers: (token: string) =>
      get<AdminUser[]>("/admin/users", undefined, token),
    toggleAdmin: (userId: number, token: string) =>
      patch<AdminUser>(`/admin/users/${userId}/toggle-admin`, undefined, token),
    deleteUser: (userId: number, token: string) =>
      del<{ status: string }>(`/admin/users/${userId}`, undefined, token),
    getPendingCount: (token: string) =>
      get<{ pending: number }>("/admin/requests/pending-count", undefined, token),
    getRequests: (token: string) =>
      get<MediaRequestItem[]>("/admin/requests", undefined, token),
    approveRequest: (requestId: number, token: string) =>
      post<{ status: string }>(`/admin/requests/${requestId}/approve`, undefined, token),
    rejectRequest: (requestId: number, token: string) =>
      post<{ status: string }>(`/admin/requests/${requestId}/reject`, undefined, token),
  },

  images: {
    options: (
      params: { subject_uri: string; kind: ImageKind; season_number?: number; episode_number?: number },
      token: string,
    ) => get<ImageOptionsResponse>("/images/options", params, token),
    listOverrides: (token: string) =>
      get<{ results: ImageOverride[] }>("/images/overrides", undefined, token),
    setOverride: (body: ImageOverrideInput, token: string) =>
      put<{ status: string; url: string }>("/images/override", body, token),
    clearOverride: (
      params: { subject_uri: string; kind: ImageKind; season_number?: number; episode_number?: number },
      token: string,
    ) => del<{ status: string; cleared: boolean }>("/images/override", params, token),
  },

  contentFilters: {
    get: (token: string) =>
      get<ContentFilters>("/media/content-filters", undefined, token),
    putGenres: (genres: string[], token: string) =>
      put<{ status: string; blocked_genres: string[] }>("/media/content-filters/genres", { genres }, token),
    putRatings: (ratings: string[], token: string) =>
      put<{ status: string; blocked_ratings: string[] }>("/media/content-filters/ratings", { ratings }, token),
    putKeywords: (keywords: string[], token: string) =>
      put<{ status: string; blocked_keywords: string[] }>("/media/content-filters/keywords", { keywords }, token),
    putRegexes: (regexes: string[], token: string) =>
      put<{ status: string; blocked_regexes: string[] }>("/media/content-filters/regexes", { regexes }, token),
    putLanguages: (languages: string[], mode: "blacklist" | "whitelist", token: string) =>
      put<{ status: string; filter_languages: string[]; language_filter_mode: string }>("/media/content-filters/languages", { languages, mode }, token),
  },
};

// Route TMDB and TheTVDB images through the backend image proxy (which serves
// a locally cached copy when the admin has enabled the image cache, or 302s to
// the origin otherwise). TheTVDB artwork has no size variants, so it uses the
// synthetic "tvdb" size bucket.
export function tmdbImageUrl(path: string | null | undefined, size: string = "w500"): string | null {
  if (!path) return null;
  if (path.startsWith("http://") || path.startsWith("https://")) {
    const match = /image\.tmdb\.org\/t\/p\/([^/]+)(\/.+)$/.exec(path);
    if (match) {
      return `/api/proxy/media/image/${match[1]}${match[2]}`;
    }
    // TVDB artwork: cache under the pseudo-size "tvdb" (backend redirects to
    // the original URL when the cache is disabled or the download fails)
    const tvdbMatch = /artworks\.thetvdb\.com(\/[^?#]+)/.exec(path);
    if (tvdbMatch) {
      return `/api/proxy/media/image/tvdb${tvdbMatch[1]}`;
    }
    return path;
  }
  // Already a proxy URL — a user-supplied image the backend stamped with this
  // route because override values must also work as a raw <img src>.
  if (path.startsWith("/api/proxy/media/image/")) return path;
  const cleanPath = path.startsWith("/") ? path : `/${path}`;
  return `/api/proxy/media/image/${size}${cleanPath}`;
}
