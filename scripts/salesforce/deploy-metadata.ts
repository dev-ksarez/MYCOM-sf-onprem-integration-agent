/**
 * Salesforce Metadata Deploy Script
 *
 * Deploys all custom objects and custom metadata types defined in
 * salesforce/metadata/ to a Salesforce org via the Metadata API.
 *
 * Required environment variables (set in .env):
 *   SF_LOGIN_URL      – e.g. https://login.salesforce.com or sandbox URL
 *   SF_CLIENT_ID      – Connected App Consumer Key
 *   SF_CLIENT_SECRET  – Connected App Consumer Secret
 *   SF_USERNAME       – Salesforce username
 *   SF_PASSWORD       – Salesforce password (append security token if needed)
 *
 * Usage:
 *   npx ts-node scripts/salesforce/deploy-metadata.ts
 */

import * as fs from "fs";
import * as path from "path";
import * as jsforce from "jsforce";
import * as dotenv from "dotenv";
import * as archiver from "archiver";

dotenv.config();

const METADATA_DIR = path.resolve(__dirname, "../../salesforce/metadata");
const PACKAGE_XML = path.join(METADATA_DIR, "package.xml");
const OBJECTS_DIR = path.join(METADATA_DIR, "objects");
const GLOBAL_VALUE_SETS_DIR = path.join(METADATA_DIR, "globalValueSets");
const DEPLOY_ZIP = path.join(__dirname, "../../.data/deploy-package.zip");

function buildFilteredPackageXml(objectApiNames: string[], globalValueSetApiNames: string[]): string {
  const objectMembers = objectApiNames.map((name) => `        <members>${name}</members>`).join("\n");
  const globalValueSetMembers = globalValueSetApiNames
    .map((name) => `        <members>${name}</members>`)
    .join("\n");

  const objectTypeBlock = objectApiNames.length > 0
    ? `    <types>\n${objectMembers}\n        <name>CustomObject</name>\n    </types>\n`
    : "";
  const globalValueSetTypeBlock = globalValueSetApiNames.length > 0
    ? `    <types>\n${globalValueSetMembers}\n        <name>GlobalValueSet</name>\n    </types>\n`
    : "";

  return `<?xml version="1.0" encoding="UTF-8"?>
<Package xmlns="http://soap.sforce.com/2006/04/metadata">
${objectTypeBlock}${globalValueSetTypeBlock}    <version>61.0</version>
</Package>`;
}

async function buildDeployZip(): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const outDir = path.dirname(DEPLOY_ZIP);
    if (!fs.existsSync(outDir)) {
      fs.mkdirSync(outDir, { recursive: true });
    }
    const output = fs.createWriteStream(DEPLOY_ZIP);
    const archive = archiver.default("zip", { zlib: { level: 9 } });

    output.on("close", () => {
      resolve(fs.readFileSync(DEPLOY_ZIP));
    });
    archive.on("error", reject);

    archive.pipe(output);

    const objectFilterRaw = process.env.SF_DEPLOY_OBJECTS?.trim();
    const allowedObjects = objectFilterRaw
      ? new Set(objectFilterRaw.split(",").map((item) => item.trim()).filter(Boolean))
      : null;
    const gvsFilterRaw = process.env.SF_DEPLOY_GLOBALVALUESETS?.trim();
    const allowedGlobalValueSets = gvsFilterRaw
      ? new Set(gvsFilterRaw.split(",").map((item) => item.trim()).filter(Boolean))
      : null;

    const filteredPackageRequested =
      (allowedObjects && allowedObjects.size > 0) ||
      (allowedGlobalValueSets && allowedGlobalValueSets.size > 0);

    if (filteredPackageRequested) {
      archive.append(
        buildFilteredPackageXml(
          allowedObjects ? [...allowedObjects] : [],
          allowedGlobalValueSets ? [...allowedGlobalValueSets] : []
        ),
        { name: "package.xml" }
      );
    } else {
      archive.file(PACKAGE_XML, { name: "package.xml" });
    }

    // Add all .object files inside objects/
    const objectFiles = fs.readdirSync(OBJECTS_DIR).filter((f) => {
      if (!f.endsWith(".object")) {
        return false;
      }

      if (!allowedObjects) {
        return true;
      }

      return allowedObjects.has(f.replace(/\.object$/, ""));
    });
    for (const objFile of objectFiles) {
      archive.file(path.join(OBJECTS_DIR, objFile), { name: `objects/${objFile}` });
    }

    // Add all .globalValueSet files inside globalValueSets/
    if (fs.existsSync(GLOBAL_VALUE_SETS_DIR)) {
      const gvsFiles = fs.readdirSync(GLOBAL_VALUE_SETS_DIR).filter((f) => {
        if (!f.endsWith(".globalValueSet")) {
          return false;
        }

        if (!allowedGlobalValueSets) {
          return true;
        }

        return allowedGlobalValueSets.has(f.replace(/\.globalValueSet$/, ""));
      });

      for (const gvsFile of gvsFiles) {
        archive.file(path.join(GLOBAL_VALUE_SETS_DIR, gvsFile), { name: `globalValueSets/${gvsFile}` });
      }
    }

    archive.finalize();
  });
}

async function waitForDeployResult(
  conn: jsforce.Connection,
  deployId: string
): Promise<any> {
  const POLL_INTERVAL_MS = 3000;
  const MAX_WAIT_MS = 5 * 60 * 1000; // 5 minutes
  const start = Date.now();

  while (true) {
    const result = await conn.metadata.checkDeployStatus(deployId, true);
    const status = result.status;
    console.log(
      `  Status: ${status} | Done: ${result.done} | Errors: ${result.numberComponentErrors ?? 0}`
    );
    if (result.done) {
      return result;
    }
    if (Date.now() - start > MAX_WAIT_MS) {
      throw new Error("Deployment timed out after 5 minutes.");
    }
    await new Promise((r) => setTimeout(r, POLL_INTERVAL_MS));
  }
}

async function main(): Promise<void> {
  const loginUrl = process.env.SF_LOGIN_URL;
  const clientId = process.env.SF_CLIENT_ID;
  const clientSecret = process.env.SF_CLIENT_SECRET;
  const username = process.env.SF_USERNAME;
  const password = process.env.SF_PASSWORD;

  if (!loginUrl) {
    console.error("Missing required env var: SF_LOGIN_URL");
    process.exit(1);
  }

  console.log(`Connecting to Salesforce: ${loginUrl}`);
  let conn: jsforce.Connection;

  if (clientId && clientSecret) {
    const tokenUrl = `${loginUrl.replace(/\/$/, "")}/services/oauth2/token`;
    const body = new URLSearchParams({
      grant_type: "client_credentials",
      client_id: clientId,
      client_secret: clientSecret
    });

    const tokenResponse = await fetch(tokenUrl, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body
    });

    if (!tokenResponse.ok) {
      const responseText = await tokenResponse.text();
      throw new Error(`Salesforce token request failed: ${tokenResponse.status} ${responseText}`);
    }

    const tokenData = (await tokenResponse.json()) as { access_token?: string; instance_url?: string };
    if (!tokenData.access_token || !tokenData.instance_url) {
      throw new Error("Salesforce token response is missing access_token or instance_url");
    }

    conn = new jsforce.Connection({
      instanceUrl: tokenData.instance_url,
      accessToken: tokenData.access_token
    });
    console.log(`Connected via client credentials. Instance: ${conn.instanceUrl}`);
  } else if (username && password) {
    conn = new jsforce.Connection({ loginUrl });
    await conn.login(username, password);
    console.log(`Connected via username/password. Instance: ${conn.instanceUrl}`);
  } else {
    console.error(
      "Missing credentials. Provide either SF_USERNAME+SF_PASSWORD or SF_CLIENT_ID+SF_CLIENT_SECRET."
    );
    process.exit(1);
  }

  console.log("Building deployment ZIP …");
  const zipBuffer = await buildDeployZip();
  console.log(`  ZIP size: ${(zipBuffer.length / 1024).toFixed(1)} KB`);

  console.log("Deploying metadata …");
  const deployOptions = {
    rollbackOnError: true,
    checkOnly: false,
    singlePackage: true,
  };

  const deployResult = await conn.metadata.deploy(
    zipBuffer.toString("base64"),
    deployOptions
  );
  const deployId: string = (deployResult as unknown as { id: string }).id;
  console.log(`  Deploy ID: ${deployId}`);
  console.log("Polling for result …");

  const result = await waitForDeployResult(conn, deployId);

  if (result.success) {
    console.log("\n✅ Deployment successful!");
    console.log(
      `   Components deployed: ${result.numberComponentsDeployed ?? "n/a"}`
    );
  } else {
    console.error("\n❌ Deployment failed!");
    const failures = result.details?.componentFailures ?? [];
    const failList = Array.isArray(failures) ? failures : [failures];
    for (const f of failList) {
      console.error(`  [${f.componentType}] ${f.fullName}: ${f.problem}`);
    }
    process.exit(1);
  }

  // Cleanup temp zip
  try {
    fs.unlinkSync(DEPLOY_ZIP);
  } catch {
    // ignore
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
