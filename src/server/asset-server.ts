import fs from "node:fs/promises";
import http from "node:http";
import path from "node:path";

export const UI_ASSET_VERSION = "20260519-dashboard-salesforce-blue-context-badges";

const INLINE_ASSET_FALLBACKS: Record<string, string> = {
  "/assets/login.js": `(function () {
  try {
    var currentUrl = new URL(window.location.href);
    var hadSensitiveParams = false;
    ["username", "password", "user", "pass"].forEach(function (key) {
      if (currentUrl.searchParams.has(key)) {
        currentUrl.searchParams.delete(key);
        hadSensitiveParams = true;
      }
    });
    if (hadSensitiveParams) {
      window.history.replaceState({}, document.title, currentUrl.pathname + currentUrl.search + currentUrl.hash);
    }
  } catch (_error) {
    // Ignore URL parsing failures and keep login functional.
  }

  var loginForm = document.getElementById("login-form");
  if (!loginForm) {
    return;
  }

  loginForm.addEventListener("submit", async function (event) {
    event.preventDefault();
    var username = String(document.getElementById("login-username").value || "").trim();
    var password = String(document.getElementById("login-password").value || "");
    var errorBox = document.getElementById("login-error");
    errorBox.classList.add("d-none");

    try {
      var csrfToken = String(document.querySelector('meta[name="sf-agent-csrf-token"]')?.getAttribute("content") || "");
      var response = await fetch("/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-CSRF-Token": csrfToken },
        body: JSON.stringify({ username: username, password: password })
      });
      var payload = await response.json().catch(function () {
        return { error: "Anmeldung fehlgeschlagen" };
      });
      if (!response.ok) {
        throw new Error(payload.error || "Anmeldung fehlgeschlagen");
      }
      window.location.href = "/";
    } catch (error) {
      errorBox.textContent = error.message || "Anmeldung fehlgeschlagen";
      errorBox.classList.remove("d-none");
    }
  });
})();\n`,
  "/assets/migration-oauth-callback.js": `(function () {
  function parseHashFragment(hash) {
    var fragment = String(hash || "").replace(/^#/, "");
    return new URLSearchParams(fragment);
  }

  var params = parseHashFragment(window.location.hash);
  var payload = {
    success: params.get("success") === "1",
    state: params.get("state") || "",
    error: params.get("error") || "",
    message: params.get("message") || ""
  };

  try {
    if (window.opener && !window.opener.closed) {
      window.opener.postMessage(payload, window.location.origin);
      setTimeout(function () {
        window.close();
      }, 150);
    } else {
      window.location.href = "/";
    }
  } catch (_error) {
    window.location.href = "/";
  }
})();\n`
};

export interface StaticAsset {
  filePath: string;
  contentType: string;
}

const STATIC_ASSETS: Record<string, StaticAsset> = {
  "/assets/bootstrap.min.css": {
    filePath: path.resolve(process.cwd(), "node_modules/bootstrap/dist/css/bootstrap.min.css"),
    contentType: "text/css; charset=utf-8"
  },
  "/assets/bootstrap.bundle.min.js": {
    filePath: path.resolve(process.cwd(), "node_modules/bootstrap/dist/js/bootstrap.bundle.min.js"),
    contentType: "application/javascript; charset=utf-8"
  },
  "/assets/chart.umd.js": {
    filePath: path.resolve(process.cwd(), "node_modules/chart.js/dist/chart.umd.js"),
    contentType: "application/javascript; charset=utf-8"
  },
  "/assets/style.css": {
    filePath: path.resolve(process.cwd(), "src/css/style.css"),
    contentType: "text/css; charset=utf-8"
  },
  "/assets/agent-ui.css": {
    filePath: path.resolve(process.cwd(), "src/css/agent-ui.css"),
    contentType: "text/css; charset=utf-8"
  },
  "/assets/template-store.css": {
    filePath: path.resolve(process.cwd(), "src/css/template-store.css"),
    contentType: "text/css; charset=utf-8"
  },
  "/assets/login.css": {
    filePath: path.resolve(process.cwd(), "src/css/login.css"),
    contentType: "text/css; charset=utf-8"
  },
  "/assets/login.js": {
    filePath: path.resolve(process.cwd(), "src/public/login.js"),
    contentType: "application/javascript; charset=utf-8"
  },
  "/assets/migration-oauth-callback.js": {
    filePath: path.resolve(process.cwd(), "src/public/migration-oauth-callback.js"),
    contentType: "application/javascript; charset=utf-8"
  },
  "/assets/examples/setup-file-import-export.example.json": {
    filePath: path.resolve(process.cwd(), "artifacts/file-examples/setup-file-import-export.example.json"),
    contentType: "application/json; charset=utf-8"
  }
};

export function registerStaticAsset(routePath: string, asset: StaticAsset): void {
  if (!routePath.startsWith("/assets/")) {
    throw new Error(`Static asset routes must live below /assets/: ${routePath}`);
  }
  STATIC_ASSETS[routePath] = asset;
}

export async function serveStaticAsset(pathname: string, res: http.ServerResponse): Promise<boolean> {
  const asset = STATIC_ASSETS[pathname];
  if (!asset) {
    return false;
  }

  let file: Buffer;
  try {
    file = await fs.readFile(asset.filePath);
  } catch (error) {
    const fallbackContent = INLINE_ASSET_FALLBACKS[pathname];
    if (!fallbackContent) {
      throw error;
    }
    file = Buffer.from(fallbackContent, "utf8");
  }

  res.writeHead(200, {
    "Content-Type": asset.contentType,
    "Cache-Control": "no-cache, no-store, must-revalidate",
    Pragma: "no-cache",
    Expires: "0"
  });
  res.end(file);
  return true;
}
