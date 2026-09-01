import { defineMiddleware } from "astro:middleware";
import { api } from "./lib/api";

const PUBLIC_ROUTES = ["/login", "/register", "/logout", "/oidc-callback", "/oidc-start", "/plex-start", "/plex-callback", "/site.webmanifest", "/favicon.ico", "/favicon.svg", "/apple-touch-icon.png", "/sw.js", "/offline.html"];
const PUBLIC_PREFIXES = ["/auth/activate/", "/forgot-password", "/reset-password/", "/api/proxy/webhooks/", "/api/proxy/auth/has-users", "/api/proxy/auth/bootstrap-restore", "/api/proxy/media/stream/", "/api/proxy/radarr-compat/", "/api/proxy/sonarr-compat/", "/stremio/"];
// Matches /profile/{id} (someone else's public profile page) but not the bare
// /profile page (the logged-in user's own profile management), which must stay gated.
const PUBLIC_PROFILE_PAGE_RE = /^\/profile\/\d+\/?$/;
// The profile page's <img> tag hits this proxy path directly. It has no file
// extension, so it doesn't fall under isStaticAsset below like TMDB poster
// URLs do, and needs the same admin-gated anonymous allowance as the page itself.
const PUBLIC_AVATAR_PROXY_RE = /^\/api\/proxy\/profile\/avatar\/\d+$/;
// Matches /list/{id} (someone else's public/friends-only list page). The list
// itself still enforces its own privacy_level server-side; this only decides
// whether a logged-out visitor gets past the gate at all.
const PUBLIC_LIST_PAGE_RE = /^\/list\/\d+\/?$/;
// The profile page's "See All" links for Top Rated Movies/Shows and Recently
// Watched Movies/Shows - same privacy model as PUBLIC_PROFILE_PAGE_RE above
// (the endpoint re-checks privacy itself).
const PUBLIC_TOP_RATED_PAGE_RE = /^\/top-rated-(?:movies|shows)\/\d+\/?$/;
const PUBLIC_RECENTLY_WATCHED_PAGE_RE = /^\/recently-watched-(?:movies|shows)\/\d+\/?$/;
// The read-only browse pages, allowed anonymously only when the admin has
// enabled logged-out navigation (Admin Settings) and a global TMDB key is set.
const PUBLIC_EXPLORE_PAGE_RE = /^\/(?:(?:movies|shows|search|lists|airing-today|discover)?|trending\/(?:movies|shows))\/?$/;
// Movie/episode and show/season/episode detail pages (TMDB- and TVDB-numbered
// variants), gated the same way as PUBLIC_EXPLORE_PAGE_RE above.
const PUBLIC_MEDIA_DETAIL_PAGE_RE =
  /^\/(?:media\/(?:movie|episode)\/\d+|show\/(?:tvdb\/)?\d+(?:\/season\/\d+(?:\/\d+)?)?|person\/\d+)\/?$/;
// The detail pages' "More like this" row and the person page's credits
// pagination are loaded client-side from these partials - same admin+
// global-key gate as the pages above, otherwise an anonymous fetch() here
// gets redirected to /login and its HTML gets injected into the page
// (fetch() follows redirects, so it looks like a normal 200 response).
const PUBLIC_RECOMMENDATIONS_PARTIAL_RE = /^\/partials\/recommendations\/?$/;
const PUBLIC_PERSON_CREDITS_PARTIAL_RE = /^\/partials\/person-credits\/?$/;
// The homepage's and /discover's data rows are loaded client-side straight
// from the backend proxy (not a same-origin partial), so the proxy path
// itself needs the same allowance - otherwise the fetch() gets redirected to
// /login and the section silently disappears (JSON.parse on the login page's
// HTML throws, caught by each row's own error handling).
const PUBLIC_MEDIA_ROWS_PROXY_RE =
  /^\/api\/proxy\/media\/(trending\/(movies|shows|trailers)|airing-today\/collected|on-air-today|now-playing|upcoming|top-rated-(movies|shows)|on-air-this-week|hidden-gems|streaming)\/?$/;
// API docs reveal the full endpoint surface and exact app version — admin-only,
// never public, regardless of the isStaticAsset check below (which would
// otherwise treat /openapi.json as a public static file just from its extension).
const ADMIN_ONLY_ROUTES = ["/docs", "/redoc", "/openapi.json"];

// Security headers added to every response.
// CSP is intentionally omitted — Astro's define:vars emits inline <script>
// blocks whose hashes change every build, making a static policy impractical.
const SECURITY_HEADERS: Record<string, string> = {
  "X-Frame-Options": "DENY",
  "X-Content-Type-Options": "nosniff",
  "Referrer-Policy": "strict-origin-when-cross-origin",
  "Permissions-Policy": "camera=(), microphone=(), geolocation=()",
};

export const onRequest = defineMiddleware(async (context, next) => {
  const token = context.cookies.get("token")?.value;
  const { pathname } = context.url;

  // Requests to the backend proxy carrying a Scrob API key (header or query
  // param) skip the cookie/JWT gate below — the proxy forwards the key as-is
  // (see api/proxy/[...path].ts) and the backend's own per-endpoint auth
  // dependency decides whether that key is accepted for the route.
  const hasApiKey =
    pathname.startsWith("/api/proxy/") &&
    (context.request.headers.get("X-Api-Key") !== null ||
      context.url.searchParams.has("api_key") ||
      context.url.searchParams.has("apikey"));

  // Skip auth for static assets and public routes
  const isStaticAsset = /\.(js|css|woff2?|ico|png|svg|webp|jpg|jpeg|webmanifest|json|xml)$/.test(pathname);

  const isAdminOnlyRoute = ADMIN_ONLY_ROUTES.includes(pathname);
  const isPublicRoute =
    !isAdminOnlyRoute &&
    (hasApiKey || isStaticAsset || PUBLIC_ROUTES.includes(pathname) || PUBLIC_PREFIXES.some(p => pathname.startsWith(p)));

  // Anonymous access to any of these read-only pages is allowed only when the
  // admin has enabled logged-out navigation (Admin Settings) and a global
  // TMDB key is set. Profile/list pages still enforce their own privacy
  // (public/friends/private) server side - this only decides whether a
  // logged-out visitor gets past the gate at all. Fails closed (redirects to
  // login) if the check errors.
  const isAllowedAnonymousPublicPage = async () => {
    const isGatedPage =
      PUBLIC_PROFILE_PAGE_RE.test(pathname) ||
      PUBLIC_AVATAR_PROXY_RE.test(pathname) ||
      PUBLIC_LIST_PAGE_RE.test(pathname) ||
      PUBLIC_TOP_RATED_PAGE_RE.test(pathname) ||
      PUBLIC_RECENTLY_WATCHED_PAGE_RE.test(pathname) ||
      PUBLIC_EXPLORE_PAGE_RE.test(pathname) ||
      PUBLIC_MEDIA_DETAIL_PAGE_RE.test(pathname) ||
      PUBLIC_RECOMMENDATIONS_PARTIAL_RE.test(pathname) ||
      PUBLIC_PERSON_CREDITS_PARTIAL_RE.test(pathname) ||
      PUBLIC_MEDIA_ROWS_PROXY_RE.test(pathname);
    if (!isGatedPage) return false;
    try {
      const status = await api.profile.publicAccessStatus();
      return status.enable_logged_out_navigation;
    } catch {
      return false;
    }
  };

  if (token) {
    try {
      // Verify token and get user info
      const user = await api.auth.me(token);
      context.locals.user = user;
      context.locals.token = token;

      // Sync primary_metadata_source cookie from user preferences
      const prefs = (user as any)?.preferences ?? (user as any)?.settings?.preferences ?? {};
      const userPref = (user as any)?.preferences?.primary_metadata_source || (user as any)?.settings?.preferences?.primary_metadata_source;
      if (userPref && context.cookies.get("primary_metadata_source")?.value !== userPref) {
        context.cookies.set("primary_metadata_source", userPref, {
          path: "/",
          maxAge: 31536000,
          sameSite: "lax",
        });
      }

      // An explicit per-user timezone beats the browser zone Base.astro
      // auto-detects, so a viewer can read a France-hosted instance in Sydney
      // time without moving anyone else's clock. tz_explicit tells that script
      // to stop auto-detecting; without a setting it keeps doing so.
      const userTz = (user as any)?.timezone || "";
      const tzExplicit = userTz ? "true" : "false";
      if (context.cookies.get("tz_explicit")?.value !== tzExplicit) {
        context.cookies.set("tz_explicit", tzExplicit, {
          path: "/",
          maxAge: 31536000,
          sameSite: "lax",
        });
      }
      if (userTz && context.cookies.get("tz")?.value !== userTz) {
        context.cookies.set("tz", userTz, {
          path: "/",
          maxAge: 31536000,
          sameSite: "lax",
        });
      }

      // Server-rendered cards read this to label Season 0 as Specials. Only write
      // it when the payload actually carried preferences — otherwise a response
      // without them resets the cookie the settings page just set.
      if ("specials_label" in prefs) {
        const specialsLabel = prefs.specials_label === true ? "true" : "false";
        if (context.cookies.get("specials_label")?.value !== specialsLabel) {
          context.cookies.set("specials_label", specialsLabel, {
            path: "/",
            maxAge: 31536000,
            sameSite: "lax",
          });
        }
      }

      // If logged in and trying to access login/register, redirect to home
      if (pathname === "/login" || pathname === "/register") {
        return context.redirect("/", 302);
      }

      // API docs are admin-only, even for logged-in non-admin users
      if (isAdminOnlyRoute && !user.is_admin) {
        return context.redirect("/", 302);
      }

      // Onboarding gate — skip for static assets, API proxy calls, and the wizard
      // routes themselves (avoid redirect loops / breaking the wizards' own fetches).
      const skipOnboardingGate =
        isStaticAsset || pathname.startsWith("/api/") || pathname === "/logout" ||
        pathname.startsWith("/setup") || pathname.startsWith("/welcome") ||
        // The welcome wizard's optional Plex-link step leaves and re-enters the
        // app via these routes — don't bounce them back to /welcome mid-flow.
        pathname.startsWith("/plex-link");
      if (!skipOnboardingGate) {
        if (user.is_admin && user.needs_setup) {
          return context.redirect("/setup", 302);
        }
        if (user.needs_onboarding) {
          return context.redirect("/welcome", 302);
        }
      }
    } catch (e) {
      // Only an auth rejection means the session is actually dead. A network
      // blip or backend restart used to delete the cookie and bounce the user
      // to /login mid-click, which looked like a random logout.
      const status = (e as any)?.status;
      const isAuthFailure = status === 401 || status === 403;
      if (isAuthFailure) {
        context.cookies.delete("token", { path: "/" });
        if (!isPublicRoute) {
          return context.redirect("/login", 302);
        }
      } else if (!isPublicRoute) {
        // Keep the session and let the page render its own error state.
        context.locals.token = token;
      }
    }
  } else {
    // No token, redirect to login if not a public route
    if (!isPublicRoute && !(await isAllowedAnonymousPublicPage())) {
      return context.redirect("/login", 302);
    }
  }

  const response = await next();
  for (const [header, value] of Object.entries(SECURITY_HEADERS)) {
    response.headers.set(header, value);
  }
  return response;
});
