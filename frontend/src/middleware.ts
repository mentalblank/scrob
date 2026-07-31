import { defineMiddleware } from "astro:middleware";
import { api } from "./lib/api";

const PUBLIC_ROUTES = ["/login", "/register", "/logout", "/oidc-callback", "/oidc-start", "/plex-start", "/plex-callback", "/site.webmanifest", "/favicon.ico", "/favicon.svg", "/apple-touch-icon.png", "/sw.js", "/offline.html"];
const PUBLIC_PREFIXES = ["/auth/activate/", "/forgot-password", "/reset-password/", "/api/proxy/webhooks/", "/api/proxy/auth/has-users", "/api/proxy/auth/bootstrap-restore", "/api/proxy/media/stream/", "/api/proxy/radarr-compat/", "/api/proxy/sonarr-compat/"];

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
  
  // Skip auth for static assets and public routes
  const isStaticAsset = /\.(js|css|woff2?|ico|png|svg|webp|jpg|jpeg|webmanifest|json|xml)$/.test(pathname);
  const isPublicRoute =
    isStaticAsset || PUBLIC_ROUTES.includes(pathname) || PUBLIC_PREFIXES.some(p => pathname.startsWith(p));

  if (token) {
    try {
      // Verify token and get user info
      const user = await api.auth.me(token);
      context.locals.user = user;
      context.locals.token = token;

      // Sync primary_metadata_source cookie from user preferences
      const userPref = (user as any)?.preferences?.primary_metadata_source || (user as any)?.settings?.preferences?.primary_metadata_source;
      if (userPref && context.cookies.get("primary_metadata_source")?.value !== userPref) {
        context.cookies.set("primary_metadata_source", userPref, {
          path: "/",
          maxAge: 31536000,
          sameSite: "lax",
        });
      }
      
      // If logged in and trying to access login/register, redirect to home
      if (pathname === "/login" || pathname === "/register") {
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
