(function () {
  "use strict";

  const TARGET_ORIGIN = "https://noesisfood.app";
  const CHANNEL_READY_TYPE = "noesisfood.license.channelReady";
  const PORT_STATE_NAME = "__NOESISFOOD_LICENSE_PORT_STATE__";
  const PORT_READY_EVENT = "noesisfood:license-port-ready";
  const MESSAGE_TYPE = "noesisfood.license.challenge";
  const RESPONSE_TYPE = "noesisfood.license.integrityToken";
  const ERROR_TYPE = "noesisfood.license.error";
  const REQUEST_HASH_RE = /^[A-Za-z0-9_-]{43}$/;
  const CHALLENGE_TOKEN_RE = /^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/;

  let nativePort = null;
  let pendingChallengeToken = "";
  let pendingRequestHash = "";
  let challengeInFlight = false;
  let sessionInFlight = false;
  let nativeLicensingStarted = false;

  function isExpectedOrigin(origin) {
    return origin === TARGET_ORIGIN;
  }

  function setStatus(key) {
    const el = document.querySelector("[data-license-status]");
    if (el) el.setAttribute("data-license-status", key);
  }

  function parseMessageData(data) {
    if (typeof data === "string") {
      try {
        return JSON.parse(data);
      } catch (_error) {
        return null;
      }
    }
    return data && typeof data === "object" ? data : null;
  }

  function isValidChallenge(challenge) {
    const requestHash = String(challenge && challenge.request_hash || "");
    const challengeToken = String(challenge && challenge.challenge_token || "");
    const expiresAt = Number(challenge && challenge.expires_at || 0);
    return REQUEST_HASH_RE.test(requestHash)
      && CHALLENGE_TOKEN_RE.test(challengeToken)
      && Number.isFinite(expiresAt)
      && expiresAt > Math.floor(Date.now() / 1000);
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
    const challenge = await response.json();
    if (!isValidChallenge(challenge)) throw new Error("malformed_challenge");
    return challenge;
  }

  function postChallengeToNative(challenge) {
    if (!nativePort || challengeInFlight) return;
    pendingChallengeToken = String(challenge.challenge_token || "");
    pendingRequestHash = String(challenge.request_hash || "");
    challengeInFlight = true;
    nativePort.postMessage(JSON.stringify({
      type: MESSAGE_TYPE,
      version: 1,
      requestHash: pendingRequestHash,
      challengeToken: pendingChallengeToken,
      expiresAt: Number(challenge.expires_at || 0),
    }));
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

  function handleNativePortMessage(event) {
    const data = parseMessageData(event.data);
    if (!data || data.version !== 1) return;
    if (data.type === ERROR_TYPE) {
      challengeInFlight = false;
      setStatus("error");
      return;
    }
    if (data.type !== RESPONSE_TYPE) return;
    const integrityToken = String(data.integrityToken || "");
    const challengeToken = String(data.challengeToken || "");
    if (!challengeInFlight || !integrityToken || challengeToken !== pendingChallengeToken || !pendingRequestHash) {
      return;
    }
    createSession(integrityToken, challengeToken).catch(() => {
      challengeInFlight = false;
      sessionInFlight = false;
      setStatus("denied");
    });
  }

  async function beginNativeLicensing(port) {
    nativePort = port;
    nativePort.onmessage = handleNativePortMessage;
    if (typeof nativePort.start === "function") nativePort.start();
    setStatus("starting");
    await clearOldAppCaches();
    const challenge = await requestChallenge();
    postChallengeToNative(challenge);
    setStatus("waiting");
  }

  function getRetainedPortState() {
    const state = window[PORT_STATE_NAME];
    return state && typeof state === "object" ? state : null;
  }

  function consumeRetainedNativePort() {
    const state = getRetainedPortState();
    if (!state || state.consumed || nativeLicensingStarted) return;
    const port = state.port;
    if (!port || typeof port.postMessage !== "function") return;
    state.consumed = true;
    state.port = null;
    nativeLicensingStarted = true;
    beginNativeLicensing(port).catch(() => {
      nativePort = null;
      pendingChallengeToken = "";
      pendingRequestHash = "";
      challengeInFlight = false;
      sessionInFlight = false;
      setStatus("browser");
    });
  }

  consumeRetainedNativePort();
  window.addEventListener(PORT_READY_EVENT, consumeRetainedNativePort);

  window.NoesisFoodLicenseBootstrap = {
    clearOldAppCaches,
    async start() {
      await clearOldAppCaches();
    },
    _test: { isExpectedOrigin, isValidChallenge, parseMessageData, consumeRetainedNativePort },
  };

  window.addEventListener("load", () => {
    clearOldAppCaches().catch(() => undefined);
  });
})();
