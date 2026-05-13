# Changelog

All notable changes to this project will be documented in this file. This project is a fork of [ellite/scrob](https://github.com/ellite/scrob).

## [Unreleased] - 2026-05-13

### Added
- **Synchronization**:
  - Implemented tiered synchronization schedules for Trakt and Media Servers (Plex, Emby, Jellyfin).
  - Support for independent Full Audit and Partial Sync intervals.
  - Background scheduler updates to handle tiered tasks.
- **Blocklist & Content Filtering**:
  - Dedicated management page for complex filtering.
  - Support for filtering by genres, keywords, and regexes.
  - Ability to block/unblock items directly from individual movie/show pages.
- **UI/UX Enhancements**:
  - **Infinite Scroll**: Replaced pagination with smooth infinite scrolling on explore and history pages.
  - **Visual Progress**: Added progress bars to MediaCards for shows and seasons.
  - **Dynamic Refresh**: "Next Up" dashboard items now refresh automatically upon episode completion.
  - **Media Integration**: Integrated trailer playback and TMDB logo support.
  - **Improved Navigation**: Sticky navbar for long pages.
  - **Hero Image Enhancements**: Added linear gradients to hero images and fallback posters for missing 
  - **Profile Improvements**: "Next Up" section added to user profile pages, sorted by recent activity.
- **Discovery & Recommendations**:
  - **Expanded Discovery**: Integration with TMDB to fetch and display streaming service providers.
  - **Discovery Auto-Pick**: Improved recommendations based on user preferences, library, and available services.
- **Integrations**:
  - **Radarr & Sonarr**: Added Radarr/Sonarr integration for auto-adding media to personal lists.
- **Safety**:
  - **Hide User Submitted Data**: Added global settings to hide user comments and ratings.
- **Maintenance**:
  - **Playback Session Cleanup**: Automated removal of abandoned playback sessions older than 24 hours.
  - **Collection Reset**: Added "Clear Collection" operation to the settings panel.
