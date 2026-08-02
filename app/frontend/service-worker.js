const CACHE_NAME = "noesisfood-shell-v3";
const APP_SHELL_ASSETS = [
  "/manifest.webmanifest",
  "/icons/icon-192.png",
  "/icons/icon-512.png",
  "/icons/icon-512-maskable.png",
];

const NEVER_CACHE_PREFIXES = [
  "/scan",
  "/scan/",
  "/scan/manual",
  "/scan/photo",
  "/feedback/correction",
  "/license",
  "/license/",
  "/internal/beta",
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(APP_SHELL_ASSETS)).catch(() => undefined)
  );
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys.map((key) => {
          if (key !== CACHE_NAME && /^noesisfood-shell-v[0-9]+$/.test(key)) {
            return caches.delete(key);
          }
          return Promise.resolve(false);
        })
      )
    )
  );
  self.clients.claim();
});

function shouldBypass(request) {
  if (!request || request.method !== "GET") return true;
  const url = new URL(request.url);
  if (url.origin !== self.location.origin) return true;
  return NEVER_CACHE_PREFIXES.some((prefix) => url.pathname === prefix || url.pathname.startsWith(prefix));
}

self.addEventListener("fetch", (event) => {
  const request = event.request;
  if (shouldBypass(request)) {
    return;
  }

  const url = new URL(request.url);

  if (request.mode === "navigate" || url.pathname === "/") {
    event.respondWith(
      fetch(request, { cache: "no-store", credentials: "same-origin" }).catch(() =>
        new Response("", { status: 503, statusText: "Offline" })
      )
    );
    return;
  }

  if (/\.(png|webmanifest|json)$/i.test(url.pathname)) {
    event.respondWith(
      caches.match(request).then((cached) => {
        if (cached) return cached;
        return fetch(request).then((response) => {
          const copy = response.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put(request, copy)).catch(() => undefined);
          return response;
        });
      })
    );
  }
});
