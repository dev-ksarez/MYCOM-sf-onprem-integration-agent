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

import * as fs from "fs";
import * as path from "path";
import * as archiver from "archiver";
import * as jsforce from "jsforce";

const SF_LOGIN_URL = process.env.SF_LOGIN_URL || "https://login.salesforce.com";
const SF_CLIENT_ID = process.env.SF_CLIENT_ID;
const SF_CLIENT_SECRET = process.env.SF_CLIENT_SECRET;
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

    archive.finalize();
  });
}

async function deployToSalesforce(): Promise<void> {
  try {
    console.log("🔐 Connecting to Salesforce...");
    
    const conn = new jsforce.Connection({
      loginUrl: SF_LOGIN_URL,
      clientId: SF_CLIENT_ID,
      clientSecret: SF_CLIENT_SECRET
    });

    // Login with OAuth
    await conn.login(SF_USERNAME, SF_PASSWORD);
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
      apiVersion: "61.0",
      rollbackOnError: true
    });

    console.log("\n📋 Deploy Status:", deployResult.status);
    console.log("   ID:", deployResult.id);

    // Wait for deployment to complete
    const deployCheck = setInterval(async () => {
      const status = await conn.metadata.checkDeployStatus(deployResult.id);
      
      if (status.done) {
        clearInterval(deployCheck);
        
        console.log("\n✅ Deployment Complete!");
        console.log("   Status:", status.status);
        console.log("   Success:", status.success);
        console.log("   Number Deployed:", status.numberComponentsDeployed);
        console.log("   Number Errors:", status.numberComponentErrors);
        
        if (status.details?.componentFailures) {
          console.log("\n❌ Errors:");
          status.details.componentFailures.forEach((failure: any) => {
            console.log(`   - ${failure.fullName}: ${failure.problem}`);
          });
        }
        
        if (status.success) {
          console.log("\n🎉 EZB__c CustomObject successfully deployed to Salesforce!");
        }
        
        process.exit(status.success ? 0 : 1);
      }
    }, 2000);

  } catch (error) {
    console.error("❌ Deployment failed:", error);
    process.exit(1);
  }
}

// Run deployment
deployToSalesforce();
