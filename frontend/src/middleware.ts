import { defineMiddleware } from "astro:middleware";
import { api } from "./lib/api";

const PUBLIC_ROUTES = ["/login", "/register", "/logout", "/oidc-callback", "/oidc-start", "/plex-start", "/plex-callback", "/site.webmanifest", "/favicon.ico", "/favicon.svg", "/apple-touch-icon.png", "/sw.js", "/offline.html"];
const PUBLIC_PREFIXES = ["/auth/activate/", "/forgot-password", "/reset-password/", "/api/proxy/webhooks/", "/api/proxy/auth/has-users", "/api/proxy/auth/bootstrap-restore", "/api/proxy/media/stream/", "/api/proxy/radarr-compat/", "/api/proxy/sonarr-compat/", "/stremio/"];
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
    if (!isPublicRoute) {
      return context.redirect("/login", 302);
    }
  }

  const response = await next();
  for (const [header, value] of Object.entries(SECURITY_HEADERS)) {
    response.headers.set(header, value);
  }
  return response;
});
