import { UI_ASSET_VERSION } from "./asset-server";

export interface PageAsset {
  href?: string;
  src?: string;
}

export function versionedAsset(path: string): string {
  return `${path}?v=${UI_ASSET_VERSION}`;
}

export function renderStylesheets(stylesheets: string[]): string {
  return stylesheets.map((href) => `    <link href="${versionedAsset(href)}" rel="stylesheet" />`).join("\n");
}

export function renderScripts(scripts: string[]): string {
  return scripts.map((src) => `    <script src="${versionedAsset(src)}"></script>`).join("\n");
}

export function renderHtmlDocument(options: {
  title: string;
  csrfToken?: string;
  stylesheets?: string[];
  scripts?: string[];
  bodyClass?: string;
  body: string;
}): string {
  const csrfMeta = options.csrfToken
    ? `    <meta name="sf-agent-csrf-token" content="${options.csrfToken}" />\n`
    : "";
  const stylesheets = renderStylesheets(options.stylesheets || []);
  const scripts = renderScripts(options.scripts || []);
  return `<!doctype html>
<html lang="de">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
${csrfMeta}    <title>${options.title}</title>
${stylesheets}
  </head>
  <body${options.bodyClass ? ` class="${options.bodyClass}"` : ""}>
${options.body}
${scripts ? `\n${scripts}` : ""}
  </body>
</html>`;
}
