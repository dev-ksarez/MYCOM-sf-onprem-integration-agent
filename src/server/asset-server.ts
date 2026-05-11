import fs from "node:fs/promises";
import http from "node:http";
import path from "node:path";

export const UI_ASSET_VERSION = "20260511-maintainability-1";

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

  const file = await fs.readFile(asset.filePath);
  res.writeHead(200, {
    "Content-Type": asset.contentType,
    "Cache-Control": "no-cache, no-store, must-revalidate",
    Pragma: "no-cache",
    Expires: "0"
  });
  res.end(file);
  return true;
}
