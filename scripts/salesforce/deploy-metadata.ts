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
const DEPLOY_ZIP = path.join(__dirname, "../../.data/deploy-package.zip");

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

    // Add package.xml at root
    archive.file(PACKAGE_XML, { name: "package.xml" });

    // Add all .object files inside objects/
    const objectFiles = fs.readdirSync(OBJECTS_DIR).filter((f) => f.endsWith(".object"));
    for (const objFile of objectFiles) {
      archive.file(path.join(OBJECTS_DIR, objFile), { name: `objects/${objFile}` });
    }

    archive.finalize();
  });
}

async function waitForDeployResult(
  conn: jsforce.Connection,
  deployId: string
): Promise<jsforce.DeployResult> {
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

  if (!loginUrl || !username || !password) {
    console.error(
      "Missing required env vars: SF_LOGIN_URL, SF_USERNAME, SF_PASSWORD"
    );
    process.exit(1);
  }

  console.log(`Connecting to Salesforce: ${loginUrl}`);
  const conn = new jsforce.Connection({
    loginUrl,
    clientId: clientId ?? undefined,
    clientSecret: clientSecret ?? undefined,
  });

  await conn.login(username, password);
  console.log(`Connected. Instance: ${conn.instanceUrl}`);

  console.log("Building deployment ZIP …");
  const zipBuffer = await buildDeployZip();
  console.log(`  ZIP size: ${(zipBuffer.length / 1024).toFixed(1)} KB`);

  console.log("Deploying metadata …");
  const deployOptions: jsforce.DeployOptions = {
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
