(function () {
  "use strict";

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

  window.NoesisFoodLicenseBootstrap = {
    clearOldAppCaches,
    async start() {
      await clearOldAppCaches();
    },
  };

  window.addEventListener("load", () => {
    clearOldAppCaches().catch(() => undefined);
  });
})();
