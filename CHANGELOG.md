# Changelog

All notable changes to this project will be documented in this file. This project is a fork of [ellite/scrob](https://github.com/ellite/scrob).

## [Unreleased] - 2026-05-08

### Added
- **Tiered Synchronization**: Implemented multi-tiered synchronization schedules for Trakt and Media Servers (Plex, Emby, Jellyfin).
  - Support for independent Full Audit and Partial Sync intervals.
  - Background scheduler updates to handle tiered tasks.
- **Blocklist & Content Filtering**:
  - Dedicated management page for complex filtering.
  - Support for filtering by genres, keywords, and regexes.
  - Ability to block/unblock items directly from individual movie/show pages.
- **UI/UX Enhancements**:
  - **Infinite Scroll**: Replaced pagination with smooth infinite scrolling on explore and history pages.
  - **Layout Toggles**: Toggle between Grid and List views for media items.
  - **Visual Progress**: Added progress bars to MediaCards for shows and seasons.
  - **Dynamic Refresh**: "Next Up" dashboard items now refresh automatically upon episode completion.
  - **Trailers**: Integrated trailer playback directly in the UI.
  - **Logos**: Added TMDB logo support for a more cinematic look.
  - **Sticky Navbar**: Improved navigation on long pages.
- **Privacy Controls**: Global settings to hide/blur user comments and ratings.
- **Radarr & Sonarr Automation**: Automatically add items from personal lists to Radarr/Sonarr.
- **Trakt Enhancements**: Option to separate the Trakt watchlist into distinct Movies and Shows sections.
- **Profile Improvements**: "Next Up" section added to user profile pages, sorted by recent activity.
- **Playback Session Cleanup**: Automated removal of abandoned playback sessions older than 24 hours.
- **Visual Tweaks**: added linear gradients to hero images.
