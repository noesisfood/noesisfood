(function () {
  "use strict";

  const TARGET_ORIGIN = "https://noesisfood.app";
  const MESSAGE_TYPE = "noesisfood.license.challenge";
  const RESPONSE_TYPE = "noesisfood.license.integrityToken";
  const ERROR_TYPE = "noesisfood.license.error";
  let pendingChallengeToken = "";
  let pendingRequestHash = "";
  let sessionInFlight = false;

  function isExpectedOrigin(origin) {
    return origin === TARGET_ORIGIN;
  }

  function setStatus(key) {
    const el = document.querySelector("[data-license-status]");
    if (el) el.setAttribute("data-license-status", key);
  }

  async function clearOldAppCaches() {
    if ("serviceWorker" in navigator) {
      const registrations = await navigator.serviceWorker.getRegistrations().catch(() => []);
      await Promise.all(registrations.map((registration) => registration.unregister().catch(() => false)));
    }
    if ("caches" in window) {
      const keys = await caches.keys().catch(() => []);
      await Promise.all(
        keys
          .filter((key) => /^noesisfood-shell-v[0-9]+$/.test(key) || key.includes("application-shell"))
          .map((key) => caches.delete(key).catch(() => false))
      );
    }
  }

  async function requestChallenge() {
    const response = await fetch("/license/challenge", {
      method: "GET",
      credentials: "same-origin",
      cache: "no-store",
      headers: { Accept: "application/json" },
    });
    if (!response.ok) throw new Error("challenge_unavailable");
    return response.json();
  }

  function postChallengeToNative(challenge) {
    pendingChallengeToken = String(challenge.challenge_token || "");
    pendingRequestHash = String(challenge.request_hash || "");
    const message = {
      type: MESSAGE_TYPE,
      version: 1,
      requestHash: pendingRequestHash,
      challengeToken: pendingChallengeToken,
      expiresAt: Number(challenge.expires_at || 0),
    };
    window.postMessage(JSON.stringify(message), TARGET_ORIGIN);
  }

  async function createSession(integrityToken, challengeToken) {
    if (sessionInFlight) return;
    sessionInFlight = true;
    const response = await fetch("/license/session", {
      method: "POST",
      credentials: "same-origin",
      cache: "no-store",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify({ integrity_token: integrityToken, challenge_token: challengeToken }),
    });
    if (!response.ok) throw new Error("session_denied");
    window.location.reload();
  }

  window.addEventListener("message", (event) => {
    if (!isExpectedOrigin(event.origin)) return;
    let data = event.data;
    if (typeof data === "string") {
      try {
        data = JSON.parse(data);
      } catch (_error) {
        return;
      }
    }
    if (!data || typeof data !== "object") return;
    if (data.type === ERROR_TYPE) {
      setStatus("error");
      return;
    }
    if (data.type !== RESPONSE_TYPE) return;
    const integrityToken = String(data.integrityToken || "");
    const challengeToken = String(data.challengeToken || "");
    if (!integrityToken || challengeToken !== pendingChallengeToken || !pendingRequestHash) return;
    createSession(integrityToken, challengeToken).catch(() => setStatus("denied"));
  });

  window.NoesisFoodLicenseBootstrap = {
    clearOldAppCaches,
    async start() {
      setStatus("starting");
      await clearOldAppCaches();
      const challenge = await requestChallenge();
      postChallengeToNative(challenge);
      setStatus("waiting");
    },
    _test: { isExpectedOrigin },
  };

  window.addEventListener("load", () => {
    clearOldAppCaches().catch(() => undefined);
    requestChallenge().then(postChallengeToNative).catch(() => setStatus("browser"));
  });
})();
