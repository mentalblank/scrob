# Changelog

All notable changes to this project will be documented in this file. This project is a fork of [ellite/scrob](https://github.com/ellite/scrob).

## [Unreleased] - 2026-05-19

### Added

#### Error Pages
- **Custom Error Pages**: Creatd 404 (Not Found) and 500 (Internal Server Error) error pages.

#### Content Filtering
- **Blocklist**: Dedicated management page for complex filtering with support for genres, keywords, and regexes.
- **Drop Show Feature**:
  - Ability to 'drop' shows from the "Next Up" list without losing watch history.
  - Added "Drop/Resume" action button to show detail pages.
  - New "Dropped Shows" section on the Content Filters page for managing dropped items.
- **User Data Visibility**: Added global settings to hide user comments and ratings.

#### Discovery
- **Streaming Providers**: Integrated with TMDB to fetch and display streaming service providers on media detail pages.

#### Lists & Collections
- **Clean List Action**: Added a list cleanup tool that detects and automatically removes items that are already in the user's collection from their custom lists.
- **Collection Reset**: Added "Clear Collection" operation to the settings panel.
- **Integrations**: Added Radarr/Sonarr integration for auto-adding media to personal lists.

#### Profiles
- **Continue Watching Page**: Dedicated dashboard route (`/continue-watching`) showing active in-progress user viewing sessions with interactive cards.
- **Next Up Display**: "Next Up" section added to user profile pages, sorted by recent activity.

#### Season Remapping
- **Remapping Manager**:
  - Dedicated "Season Remapping" page (`/remaps`) to manage metadata sync overrides.
  - Built-in TMDB TV show search and season-selection wizard inside the creation flow.

#### Sync & Backend
- **Intervals**: Support for independent Full Audit and Partial Sync intervals for Trakt and Media Servers (Plex, Emby, Jellyfin).
- **Session Cleanup**: Automated removal of abandoned playback sessions older than 24 hours.
- **Version Labeling**: Added branch-based and build-based version tag parameters to the `/about` metadata page.

#### UI/UX
- **Infinite Scroll**: Replaced pagination with smooth infinite scrolling on explore and history pages (toggleable to paginated view in settings).
- **Media Integration**: Integrated trailer playback and TMDB logo support.
- **Visual Progress**: Added progress bars to MediaCards for shows and seasons.

---

### Changed

#### Discovery
- **Auto-Pick Recommendations**: Improved recommendations based on user preferences, library, and available services.

#### Sync & Backend
- **Dynamic Refresh**: "Next Up" dashboard items now refresh automatically upon episode completion.
- **Next Up Dashboard Query**: Optimized and refactored the unwatched episode SQL retrieval query to improve page load speed on large databases.

#### UI/UX
- **Hero Images**: Added linear gradients to hero images.
- **Navigation Bar**: Sticky navbar for long pages.

---

### Fixed

#### UI/UX
- **Favicon**: Replaced the Astro favicon assets across all formats (PNG, SVG, ICO, Manifest) with the Scrob logo.

#### Sync & Backend
- **Plex Library Synchronization**: Implemented pagination chunking of 500 records per page for movie, show, and episode requests to prevent OOM/timeouts on large Plex libraries.