# Changelog

All notable changes to this project will be documented in this file. This project is a fork of [ellite/scrob](https://github.com/ellite/scrob).

## [Unreleased]

### Added

- **Connections Page**: Media servers, trackers and integrations moved out of Settings onto a dedicated `/connections` page with its own tabs and save button.
- **Data Import**: Import a Scrob data export or a Trakt export zip from Connections → Import.
- **Data Export**: Download watch history, ratings, collection, lists and comments as a zip from Settings → Maintenance & Data.
- **Next Up Display Options**: Shuffle the Next Up row, or render it as a plain row without the banner.
- **Series Progress Page**: A `/progress` page listing every show you have started, with watched/aired episode counts and a progress bar, sortable by recency, completeness, episodes remaining or title.
- **Compact Watch History**: Toggle the history page between card and compact list views; the choice is remembered per browser.
- **Change Username**: Usernames can be edited from Profile settings, with uniqueness and format validation.
- **Stremio Configure Link**: The addon's configuration page is reachable from Integrations settings.
- **Maintenance & Data Tab**: Maintenance and data-management actions moved out of Security & API Keys into their own settings tab.
- **Registration Controls**: Admins can allow or block new registrations and set a maximum user count from Global Server Settings, without editing environment variables or restarting. Leaving them unset keeps the existing `ENABLE_REGISTRATIONS` / `REGISTRATION_MAX_ALLOWED_USERS` behaviour.
- **First-Run Setup Wizard**: Fresh installs land on registration, the first account becomes admin, and admins are walked through API keys before anything else.
- **Onboarding Walkthrough**: Register offers Plex sign-up, email, or email-plus-link-later, followed by a skippable settings walkthrough (profile, appearance, region, genres, content preferences, optional Plex link).
- **Content Rating Blocklist**: Block titles by age certification (TV-MA, R, MA15+, +18 and others). Ratings are region-aware, derived from the profile country, and any value can be added by hand.
- **Image Cache Expiration**: Set how long cached artwork is kept, alongside the existing size limit.
- **Episode to Movie Matching**: Link an episode that a catalogue files as a film to its TMDB movie without removing it from the season, with a "Matched as Movies" list and one-click revert.
- **Dismiss Sync Warnings**: Hide unmatched items that can't be fixed, on both the remaps page and the media servers panel, with a restore action.
- **Content Rating Backfill**: Admin maintenance action to fetch certifications for the existing library.
- **Plex Account Login**: Sign in with a Plex account via the plex.tv PIN flow, link/unlink Plex from existing accounts, and pick which discovered Plex servers to import as media-server connections.
- **Language Filters**: Added a new content filter section allowing users to blacklist or whitelist specific languages to exclude/include them in discovery results.
- **Media Server Enrichment Toggle**: Added a setting to enable/disable metadata enrichment (resolution, languages) from Plex, Jellyfin, and Emby.
- **Watch History Features**: Added date range filtering, grouping options (Date, Media Type, Show/Movie, Season), and single-click history event deletion directly from the `/history` page.
- **Play History Management**: Added the ability to click the play count badge on movie and episode pages to open a modal, view specific watch history events, and delete individual plays.
- **Custom Error Pages**: Created custom 404 (Not Found) and 500 (Internal Server Error) error pages.
- **Blocklist**: Added a dedicated filtering page for genres, keywords, and regex patterns.
- **Drop Show**: Added the ability to drop/resume shows from the Next Up list without losing history.
- **User Data Visibility**: Added global settings to toggle the visibility of comments and ratings.
- **Streaming Providers**: Display streaming availability on media detail pages using TMDB.
- **Clean List Action**: Added a list cleanup tool to automatically remove already-collected items from custom lists.
- **Collection Reset**: Added a "Clear Collection" action to the settings panel.
- **Integrations**: Integrated Radarr/Sonarr to automatically add media from personal lists.
- **Continue Watching**: Created a dedicated dashboard page showing active in-progress viewing sessions.
- **Next Up Display**: Added a "Next Up" section to user profile pages, sorted by recent activity.
- **Season Remapping**: Created a `/remaps` page with TMDB search and a selection wizard.
- **Sync Intervals**: Supported independent Full/Partial sync intervals for Trakt and Media Servers.
- **Session Cleanup**: Automated removal of abandoned playback sessions older than 24 hours.
- **Version Labeling**: Added branch and build version tag parameters to the `/about` metadata page.
- **Infinite Scroll**: Replaced pagination with toggleable infinite scrolling on explore and history pages.
- **Media Integration**: Integrated video trailer dialogs and TMDB logo displays.
- **Visual Progress**: Added watch progress bars to show and season cards.
- **Streaming Providers**: Integrated with TMDB to fetch and display streaming service providers on media detail pages.

---

### Changed

- **Next Up**: Includes any show with watch progress rather than only collected items, and lists only episodes that have aired.
- **Image Cache Coverage**: Hero images, posters, people, collections, requests and logos bypassed the cache entirely, and several valid TMDB sizes were rejected.
- **Episode Order Switching**: Switching a matched show between TMDB and TVDB navigates straight to the other view instead of running an episode mapping job first.
- **Remap Targets**: A remap can point at a show that isn't in the library yet; it is fetched and created on demand.
- **Sync Warnings**: Each warning states which catalogue the lookup failed against, and warnings whose episodes are already identified are no longer reported.
- **Detail Page Action Buttons**: Redesigned buttons across movie, show, and episode detail pages into a unified, dynamically scaling grid to eliminate empty margins.
- **Discover and Airing Today Cards**: Aligned discover page grids and home page airing today cards to use the vertical 2:3 `MediaCard` poster layout.
- **Single-Season Breadcrumbs**: Simplified breadcrumbs on episode detail pages to skip the intermediate season level for single-season shows.
- **Show Page Images**: Restored original poster/backdrop behavior in backend queries.
- **Season and Episode Pages**: Restructured Movie, Show, Season, and Episode detail pages into a two-column sidebar layout.
- **Empty Season Filtering**: Excluded seasons with zero episodes from show detail pages and the season selector navigation.
- **Cross-Season Episode Navigation**: Changed episode page pagination to allow navigating to the next season's first episode or the previous season's last episode when reaching season boundaries.
- **Hero/Backdrop Images**: Unified the layout, scaling, masking, height, and backdrop opacity of hero images on the Season detail, Episode detail, and Next Up dashboard sections to match the Show page backdrop behavior.
- **Settings Tab Panels**: Reorganised settings page to use a tab layout.
- **UI Redesign**: Overhauled the frontend visual styling system using modern glassmorphic card overlays, typography (Inter and Plus Jakarta Sans), and ambient background glows
- **Mobile Detail Pages**: Cleaned up layout structure, margins, and typography for show, movie, season, and episode detail pages on mobile viewports. Relocated production/network logos inside the metadata info box, optimized padding and font sizes of tags, hid secondary SVG icons to save screen space, and reduced overview description typography scale.
- **History Card Badges**: Removed redundant blue season/episode badge and green checkmark badge overlay on watch history cards.
- **Mobile Details Alignment**: Centered logos, titles, action bars, metadata, and overview text blocks when viewed on mobile viewports for show, season, episode, and movie detail pages.
- **Continue Watching Cards**: Replaced the vertical card design with landscape episode cards across homepage and continue-watching lists.
- **Season Episode Progress**: Integrated database-backed watch progress bars directly onto season episode cards.
- **Auto-Pick Recommendations**: Improved suggestions based on preferences, library, and available services.
- **Dynamic Refresh**: Enabled automatic refreshing of Next Up dashboard items upon episode completion.
- **Next Up Query**: Optimized SQL unwatched episode retrieval queries for large databases.
- **Hero Images**: Added smooth linear gradients to media detail heroes.
- **Navigation Bar**: Made the main navigation bar sticky for better usability.

---

### Fixed

- **Favicon**: Replaced default Astro favicons across all asset formats with the custom Scrob logo.
- **Plex Sync**: Chunked synchronization requests into 500-record pages to prevent OOM errors on large libraries.
