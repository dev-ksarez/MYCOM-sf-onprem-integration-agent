/**
 * Quick Deploy EZB__c CustomObject to Salesforce
 * 
 * Usage:
 *   npx ts-node scripts/salesforce/deploy-ezb.ts
 * 
 * Requirements:
 *   - Environment variables: SF_LOGIN_URL, SF_CLIENT_ID, SF_CLIENT_SECRET
 *   - Salesforce org with API access
 */

import fs from "node:fs";
import path from "node:path";
import archiver from "archiver";
import { Connection } from "jsforce";

const SF_LOGIN_URL = process.env.SF_LOGIN_URL || "https://login.salesforce.com";
const SF_USERNAME = process.env.SF_USERNAME;
const SF_PASSWORD = process.env.SF_PASSWORD;

const METADATA_DIR = path.resolve(__dirname, "../../salesforce/metadata");
const DEPLOY_ZIP = path.join(__dirname, "../../.data/deploy-ezb.zip");

async function createDeployZip(): Promise<void> {
  return new Promise((resolve, reject) => {
    const output = fs.createWriteStream(DEPLOY_ZIP);
    const archive = archiver("zip", { zlib: { level: 9 } });

    archive.on("error", reject);
    output.on("close", resolve);

    archive.pipe(output);

    // Add package.xml
    archive.file(path.join(METADATA_DIR, "package.xml"), { name: "package.xml" });

    // Add EZB__c object and related metadata
    archive.file(path.join(METADATA_DIR, "objects/EZB__c.object"), {
      name: "objects/EZB__c.object"
    });
    
    archive.file(path.join(METADATA_DIR, "tabs/EZB__c.tab"), {
      name: "tabs/EZB__c.tab"
    });

    void archive.finalize();
  });
}

function requireEnv(name: string, value: string | undefined): string {
  const normalized = String(value || "").trim();
  if (!normalized) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return normalized;
}

async function wait(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function deployToSalesforce(): Promise<void> {
  try {
    console.log("🔐 Connecting to Salesforce...");
    
    const conn = new Connection({ loginUrl: SF_LOGIN_URL });

    const username = requireEnv("SF_USERNAME", SF_USERNAME);
    const password = requireEnv("SF_PASSWORD", SF_PASSWORD);

    // Login with OAuth
    await conn.login(username, password);
    console.log("✅ Connected to Salesforce");

    // Create deployment ZIP
    console.log("📦 Creating deployment package...");
    await createDeployZip();
    console.log("✅ ZIP created:", DEPLOY_ZIP);

    // Read ZIP file
    const zipBuffer = fs.readFileSync(DEPLOY_ZIP);
    const zipBase64 = zipBuffer.toString("base64");

    // Deploy via Metadata API
    console.log("🚀 Deploying EZB__c CustomObject...");
    const deployResult = await conn.metadata.deploy(zipBase64, {
      rollbackOnError: true
    });

    console.log("\n📋 Deployment ID:", deployResult.id);

    if (!deployResult.id) {
      throw new Error("Metadata deploy did not return a deployment id");
    }

    // Wait for deployment to complete.
    for (let attempt = 0; attempt < 180; attempt += 1) {
      const status = await conn.metadata.checkDeployStatus(deployResult.id, true);
      if (status.done) {
        console.log("\n✅ Deployment Complete!");
        console.log("   Status:", status.status);
        console.log("   Success:", status.success);
        console.log("   Number Deployed:", status.numberComponentsDeployed);
        console.log("   Number Errors:", status.numberComponentErrors);

        const failures = Array.isArray(status.details?.componentFailures)
          ? status.details?.componentFailures
          : status.details?.componentFailures
            ? [status.details.componentFailures]
            : [];

        if (failures.length > 0) {
          console.log("\n❌ Errors:");
          failures.forEach((failure) => {
            console.log(`   - ${failure.fullName || "unknown"}: ${failure.problem || "unknown error"}`);
          });
        }

        if (status.success) {
          console.log("\n🎉 EZB__c CustomObject successfully deployed to Salesforce!");
        }

        process.exit(status.success ? 0 : 1);
      }

      await wait(2000);
    }

    throw new Error("Timed out waiting for metadata deployment result");

  } catch (error) {
    console.error("❌ Deployment failed:", error);
    process.exit(1);
  }
}

// Run deployment
deployToSalesforce();
