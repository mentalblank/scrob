import type { APIRoute } from "astro";

const BACKEND_PORT = (import.meta.env.BACKEND_PORT as string | undefined) ?? "7331";
const BACKEND = `http://localhost:${BACKEND_PORT}`;

export const ALL: APIRoute = async ({ params, request }) => {
  const path = params.path ?? "";
  const search = new URL(request.url).search;
  const backendUrl = `${BACKEND}/stremio/${path}${search}`;

  const forwardHeaders = new Headers();
  
  // Forward original host and protocol headers
  const reqUrl = new URL(request.url);
  const host = request.headers.get("x-forwarded-host") || request.headers.get("host") || reqUrl.host;
  const proto = request.headers.get("x-forwarded-proto") || reqUrl.protocol.replace(":", "");

  forwardHeaders.set("X-Forwarded-Host", host);
  forwardHeaders.set("X-Forwarded-Proto", proto);

  const ct = request.headers.get("Content-Type");
  if (ct) forwardHeaders.set("Content-Type", ct);

  const hasBody = request.method !== "GET" && request.method !== "HEAD";
  const body = hasBody ? await request.arrayBuffer() : undefined;

  try {
    const res = await fetch(backendUrl, {
      method: request.method,
      headers: forwardHeaders,
      body,
    });

    const responseHeaders = new Headers(res.headers);
    responseHeaders.set("Access-Control-Allow-Origin", "*");
    responseHeaders.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    responseHeaders.set("Access-Control-Allow-Headers", "*");

    return new Response(res.body, { status: res.status, headers: responseHeaders });
  } catch (e: any) {
    return new Response(JSON.stringify({ error: "Backend service error", detail: e?.message }), {
      status: 502,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
      },
    });
  }
};

export const GET = ALL;
export const POST = ALL;
export const OPTIONS = ALL;
