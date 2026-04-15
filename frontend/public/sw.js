// Scrob — Service Worker
// Strategy:
//   - Static assets (JS/CSS/fonts/icons): NetworkFirst, cached for offline fallback
//   - TMDB images: CacheFirst, max 500 entries / 30 days
//   - /api/proxy/*: NetworkOnly — library data must always be fresh
//   - Navigation (HTML pages): NetworkFirst, offline fallback if all fail

const SHELL_CACHE  = 'scrob-shell-v2';
const IMAGE_CACHE  = 'scrob-images-v1';
const IMAGE_MAX    = 500;
const IMAGE_TTL_MS = 30 * 24 * 60 * 60 * 1000; // 30 days

// ── Install ───────────────────────────────────────────────────────────────────
self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(SHELL_CACHE).then(c => c.add('/offline.html'))
  );
  self.skipWaiting();
});

// ── Activate — prune old caches ───────────────────────────────────────────────
self.addEventListener('activate', (event) => {
  const keep = [SHELL_CACHE, IMAGE_CACHE];
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => !keep.includes(k)).map(k => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

// ── Fetch ─────────────────────────────────────────────────────────────────────
self.addEventListener('fetch', (event) => {
  const { request } = event;
  const url = new URL(request.url);

  // Only handle GET — leave POST/PATCH/DELETE to the network
  if (request.method !== 'GET') return;

  // API calls: always network, never cache
  if (url.pathname.startsWith('/api/')) {
    return; // fall through to browser default (network)
  }

  // TMDB images: cache-first with expiry + size cap
  if (url.hostname === 'image.tmdb.org') {
    event.respondWith(tmdbImageStrategy(request));
    return;
  }

  // Web manifest: NetworkFirst to handle potential auth redirects / CORS correctly
  if (url.pathname.endsWith('.webmanifest')) {
    event.respondWith(networkFirstWithOffline(request));
    return;
  }

  // Static assets (hashed JS/CSS/fonts/icons in /_astro/): network-first so
  // content is always fresh (avoids stale cache in dev and after deploys).
  // Cache is kept as an offline fallback only.
  if (url.pathname.startsWith('/_astro/') || isStaticAsset(url.pathname)) {
    event.respondWith(networkFirstWithCache(request, SHELL_CACHE));
    return;
  }

  // Navigation (HTML pages): network-first, offline fallback
  if (request.mode === 'navigate') {
    event.respondWith(networkFirstWithOffline(request));
    return;
  }
});

// ── Strategies ────────────────────────────────────────────────────────────────

async function networkFirstWithCache(request, cacheName) {
  try {
    const response = await fetch(request);
    if (response.ok) {
      const cache = await caches.open(cacheName);
      cache.put(request, response.clone());
    }
    return response;
  } catch {
    const cached = await caches.match(request);
    return cached ?? Response.error();
  }
}

async function networkFirstWithOffline(request) {
  try {
    const response = await fetch(request);
    return response;
  } catch {
    const cached = await caches.match(request);
    if (cached) return cached;
    return caches.match('/offline.html');
  }
}

async function tmdbImageStrategy(request) {
  const cache = await caches.open(IMAGE_CACHE);
  const cached = await cache.match(request);

  if (cached) {
    const cachedDate = cached.headers.get('sw-cached-at');
    if (cachedDate && Date.now() - Number(cachedDate) < IMAGE_TTL_MS) {
      return cached;
    }
    // Expired — fall through to network
  }

  try {
    const response = await fetch(request);
    if (response.ok) {
      // Inject timestamp header so we can check expiry later
      const headers = new Headers(response.headers);
      headers.set('sw-cached-at', String(Date.now()));
      const toStore = new Response(await response.clone().arrayBuffer(), {
        status: response.status,
        statusText: response.statusText,
        headers,
      });
      await cache.put(request, toStore);
      await trimCache(cache, IMAGE_MAX);
    }
    return response;
  } catch {
    return cached ?? Response.error();
  }
}

async function trimCache(cache, maxEntries) {
  const keys = await cache.keys();
  if (keys.length > maxEntries) {
    await Promise.all(keys.slice(0, keys.length - maxEntries).map(k => cache.delete(k)));
  }
}

function isStaticAsset(pathname) {
  return /\.(js|css|woff2?|ico|png|svg|webp|jpg|jpeg|webmanifest)$/.test(pathname);
}
