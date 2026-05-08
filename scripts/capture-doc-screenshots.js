const fs = require("node:fs");
const path = require("node:path");
const { spawn } = require("node:child_process");

const chromePath = process.env.CHROME_BIN || "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
const baseUrl = process.env.DOC_SCREENSHOT_BASE_URL || "http://127.0.0.1:18081";
const username = process.env.DOC_SCREENSHOT_USERNAME || "admin";
const password = process.env.DOC_SCREENSHOT_PASSWORD || "admin123!";
const outputDir = path.resolve(process.cwd(), "docs/screenshots");
const confluenceOutputDir = path.resolve(process.cwd(), "docs/confluence/assets/screenshots");
const debugPort = Number(process.env.DOC_SCREENSHOT_DEBUG_PORT || 9223);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForEndpoint(url, timeoutMs = 10000) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    try {
      const response = await fetch(url);
      if (response.ok) {
        return await response.json();
      }
    } catch {
      // Chrome is still starting.
    }
    await sleep(150);
  }
  throw new Error(`Timed out waiting for ${url}`);
}

class CdpClient {
  constructor(wsUrl) {
    this.nextId = 1;
    this.pending = new Map();
    this.events = new Map();
    this.socket = new WebSocket(wsUrl);
  }

  async open() {
    await new Promise((resolve, reject) => {
      this.socket.addEventListener("open", resolve, { once: true });
      this.socket.addEventListener("error", reject, { once: true });
    });
    this.socket.addEventListener("message", (event) => {
      const message = JSON.parse(String(event.data));
      if (message.id && this.pending.has(message.id)) {
        const { resolve, reject } = this.pending.get(message.id);
        this.pending.delete(message.id);
        if (message.error) {
          reject(new Error(message.error.message || "CDP command failed"));
        } else {
          resolve(message.result || {});
        }
        return;
      }
      if (message.method && this.events.has(message.method)) {
        for (const handler of this.events.get(message.method)) {
          handler(message.params || {});
        }
      }
    });
  }

  send(method, params = {}) {
    const id = this.nextId++;
    const commandLabel = method;
    this.socket.send(JSON.stringify({ id, method, params }));
    return new Promise((resolve, reject) => {
      this.pending.set(id, {
        resolve,
        reject: (error) => {
          error.message = `${commandLabel}: ${error.message}`;
          reject(error);
        }
      });
    });
  }

  once(method) {
    return new Promise((resolve) => {
      const handler = (params) => {
        const handlers = this.events.get(method) || [];
        this.events.set(method, handlers.filter((candidate) => candidate !== handler));
        resolve(params);
      };
      const handlers = this.events.get(method) || [];
      handlers.push(handler);
      this.events.set(method, handlers);
    });
  }

  close() {
    this.socket.close();
  }
}

async function navigate(client, url) {
  const loaded = client.once("Page.loadEventFired");
  await client.send("Page.navigate", { url });
  await loaded;
  await sleep(800);
}

async function screenshot(client, fileName) {
  const result = await client.send("Page.captureScreenshot", {
    format: "png",
    captureBeyondViewport: false,
    fromSurface: true
  });
  const imageBuffer = Buffer.from(result.data, "base64");
  fs.writeFileSync(path.join(outputDir, fileName), imageBuffer);
  fs.mkdirSync(confluenceOutputDir, { recursive: true });
  fs.writeFileSync(path.join(confluenceOutputDir, fileName), imageBuffer);
}

async function screenshotMany(client, fileNames) {
  const result = await client.send("Page.captureScreenshot", {
    format: "png",
    captureBeyondViewport: false,
    fromSurface: true
  });
  const imageBuffer = Buffer.from(result.data, "base64");
  fs.mkdirSync(confluenceOutputDir, { recursive: true });
  for (const fileName of fileNames) {
    fs.writeFileSync(path.join(outputDir, fileName), imageBuffer);
    fs.writeFileSync(path.join(confluenceOutputDir, fileName), imageBuffer);
  }
}

async function showTab(client, selector) {
  await client.send("Runtime.evaluate", {
    awaitPromise: true,
    expression: `
      (async () => {
        const trigger = document.querySelector(${JSON.stringify(selector)});
        if (!trigger) return false;
        trigger.classList.remove('d-none');
        trigger.click();
        await new Promise((resolve) => setTimeout(resolve, 900));
        return true;
      })()
    `
  });
  await sleep(500);
}

async function showSchedulerWizardStep(client, step) {
  await client.send("Runtime.evaluate", {
    awaitPromise: true,
    expression: `
      (async () => {
        const newButton = document.querySelector('#new-schedule');
        if (newButton && !document.querySelector('#schedule-modal.show')) {
          newButton.click();
          await new Promise((resolve) => setTimeout(resolve, 900));
        }
        const stepButton = document.querySelector('[data-sch-step="${String(step)}"]');
        const stepPanel = document.querySelector('[data-sch-step-panel="${String(step)}"]');
        if (!stepButton || !stepPanel) return false;
        document.querySelectorAll('[data-sch-step]').forEach((button) => {
          const buttonStep = Number(button.getAttribute('data-sch-step') || '0');
          button.classList.toggle('is-active', button === stepButton);
          button.classList.toggle('is-complete', buttonStep > 0 && buttonStep < ${String(step)});
        });
        document.querySelectorAll('[data-sch-step-panel]').forEach((panel) => {
          const isActive = panel === stepPanel;
          panel.classList.toggle('show', isActive);
          panel.classList.toggle('active', isActive);
          panel.classList.toggle('d-none', !isActive);
        });
        const hint = document.querySelector('#sch-wizard-hint');
        if (hint) {
          const labels = { 2: 'Quelle', 3: 'Ziel', 5: 'Mapping' };
          hint.textContent = 'Dokumentationsansicht: Schritt ' + ${String(step)} + ' - ' + (labels[${String(step)}] || 'Scheduler-Assistent') + '.';
        }
        await new Promise((resolve) => setTimeout(resolve, 900));
        return true;
      })()
    `
  });
  await sleep(500);
}

async function main() {
  if (!fs.existsSync(chromePath)) {
    throw new Error(`Chrome not found at ${chromePath}`);
  }

  fs.mkdirSync(outputDir, { recursive: true });
  fs.mkdirSync(confluenceOutputDir, { recursive: true });
  const profileDir = path.resolve(process.cwd(), "artifacts/chrome-doc-screenshots");
  fs.rmSync(profileDir, { recursive: true, force: true });
  fs.mkdirSync(profileDir, { recursive: true });

  const chrome = spawn(chromePath, [
    "--headless=new",
    "--disable-gpu",
    "--hide-scrollbars",
    "--no-first-run",
    "--no-default-browser-check",
    `--remote-debugging-port=${debugPort}`,
    `--user-data-dir=${profileDir}`,
    "--window-size=1440,1000",
    "about:blank"
  ], { stdio: "ignore" });

  try {
    const version = await waitForEndpoint(`http://127.0.0.1:${debugPort}/json/version`);
    const browserClient = new CdpClient(version.webSocketDebuggerUrl);
    await browserClient.open();
    const target = await browserClient.send("Target.createTarget", { url: "about:blank" });
    const targetId = target.targetId;
    if (!targetId) {
      throw new Error("Chrome target creation failed");
    }
    const pageTargets = await waitForEndpoint(`http://127.0.0.1:${debugPort}/json`);
    const pageTarget = Array.isArray(pageTargets)
      ? pageTargets.find((item) => item.id === targetId) || pageTargets.find((item) => item.type === "page")
      : null;
    if (!pageTarget?.webSocketDebuggerUrl) {
      throw new Error("Chrome page debugging endpoint not found");
    }
    const pageWsUrl = pageTarget.webSocketDebuggerUrl;
    const client = new CdpClient(pageWsUrl);
    await client.open();
    await client.send("Page.enable");
    await client.send("Runtime.enable");
    await client.send("Emulation.setDeviceMetricsOverride", {
      width: 1440,
      height: 1000,
      deviceScaleFactor: 1,
      mobile: false
    });

    await navigate(client, `${baseUrl}/`);
    await screenshot(client, "01-login.png");

    await client.send("Runtime.evaluate", {
      awaitPromise: true,
      expression: `
        (async () => {
          const csrfToken = String(document.querySelector('meta[name="sf-agent-csrf-token"]')?.getAttribute('content') || '');
          const response = await fetch('/auth/login', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': csrfToken },
            body: JSON.stringify({ username: ${JSON.stringify(username)}, password: ${JSON.stringify(password)} })
          });
          if (!response.ok) throw new Error('Login failed: ' + response.status);
          return true;
        })()
      `
    });
    await navigate(client, `${baseUrl}/`);
    await screenshot(client, "02-dashboard.png");

    await showTab(client, '[data-bs-target="#tab-schedulers"]');
    await screenshotMany(client, ["03-scheduler.png", "09-assistent-scheduler.png"]);
    await showSchedulerWizardStep(client, 2);
    await screenshotMany(client, ["11-assistent-quelle.png"]);
    await showSchedulerWizardStep(client, 3);
    await screenshotMany(client, ["12-assistent-ziel.png"]);
    await showSchedulerWizardStep(client, 5);
    await screenshotMany(client, ["13-assistent-mapping.png"]);

    await showTab(client, '[data-bs-target="#tab-connectors"]');
    await screenshotMany(client, ["04-connectoren.png", "08-assistent-connector.png"]);

    await showTab(client, '[data-bs-target="#tab-monitor"]');
    await screenshot(client, "05-monitoring.png");

    await showTab(client, '#tab-installer-trigger');
    await screenshot(client, "06-installation.png");

    await showTab(client, '[data-bs-target="#tab-migration"]');
    await screenshotMany(client, ["07-migration.png", "10-assistent-migration.png"]);

    client.close();
    browserClient.close();
  } finally {
    chrome.kill("SIGTERM");
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
