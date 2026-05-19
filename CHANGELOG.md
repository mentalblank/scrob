# Changelog

All notable changes to this project will be documented in this file. This project is a fork of [ellite/scrob](https://github.com/ellite/scrob).

## [Unreleased] - 2026-05-19

### Added
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
- **Auto-Pick Recommendations**: Improved suggestions based on preferences, library, and available services.
- **Dynamic Refresh**: Enabled automatic refreshing of Next Up dashboard items upon episode completion.
- **Next Up Query**: Optimized SQL unwatched episode retrieval queries for large databases.
- **Hero Images**: Added smooth linear gradients to media detail heroes.
- **Navigation Bar**: Made the main navigation bar sticky for better usability.

---

### Fixed
- **Favicon**: Replaced default Astro favicons across all asset formats with the custom Scrob logo.
- **Plex Sync**: Chunked synchronization requests into 500-record pages to prevent OOM errors on large libraries.