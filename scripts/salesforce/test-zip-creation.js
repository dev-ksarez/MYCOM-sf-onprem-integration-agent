#!/usr/bin/env node

const fs = require("fs");
const path = require("path");
const Archiver = require("archiver");

function appRoot() {
  return path.resolve(__dirname, "..", "..");
}

async function testZipCreation() {
  const metadataDir = path.join(appRoot(), "salesforce", "metadata");
  const packageXml = path.join(metadataDir, "package.xml");
  const objectsDir = path.join(metadataDir, "objects");

  console.log("🧪 Testing ZIP Creation...");
  console.log(`📁 Metadata dir: ${metadataDir}`);
  console.log(`📄 package.xml exists: ${fs.existsSync(packageXml)}`);
  console.log(`📁 objects dir exists: ${fs.existsSync(objectsDir)}`);

  const objectFiles = fs.readdirSync(objectsDir).filter((f) => f.endsWith(".object"));
  console.log(`\n📦 Found ${objectFiles.length} .object files:`);
  let totalSize = 0;
  for (const f of objectFiles) {
    const filePath = path.join(objectsDir, f);
    const size = fs.statSync(filePath).size;
    console.log(`   - ${f} (${size} bytes)`);
    totalSize += size;
  }

  console.log(`   Total object files size: ${totalSize} bytes`);
  console.log(`   package.xml size: ${fs.statSync(packageXml).size} bytes`);
  console.log(`   Expected ZIP size ~: ${totalSize + fs.statSync(packageXml).size} bytes (uncompressed)\n`);

  // Build the ZIP exactly like in init-installation.js
  const zipData = await new Promise((resolve, reject) => {
    const chunks = [];
    const archive = Archiver("zip", { zlib: { level: 9 } });

    archive.on("data", (chunk) => {
      chunks.push(chunk);
      console.log(`   📦 ZIP chunk received: ${chunk.length} bytes`);
    });
    
    archive.on("end", () => {
      const buffer = Buffer.concat(chunks);
      console.log(`   ✓ ZIP finalized: ${buffer.length} bytes total`);
      resolve(buffer);
    });
    
    archive.on("error", (err) => {
      reject(new Error(`ZIP creation failed: ${err.message}`));
    });

    console.log("📋 Adding package.xml...");
    archive.file(packageXml, { name: "package.xml" });

    console.log(`📦 Adding ${objectFiles.length} .object files...`);
    for (const objFile of objectFiles) {
      const filePath = path.join(objectsDir, objFile);
      archive.file(filePath, { name: `objects/${objFile}` });
    }

    console.log("🔄 Finalizing archive...");
    archive.finalize().catch(reject);
  });

  console.log(`\n✅ ZIP created successfully: ${zipData.length} bytes`);
  
  // Save for inspection
  const testZipPath = path.join(appRoot(), ".test-deploy.zip");
  fs.writeFileSync(testZipPath, zipData);
  console.log(`💾 Saved to: ${testZipPath}\n`);

  // Try to inspect the ZIP contents
  try {
    const AdmZip = require("adm-zip");
    const zip = new AdmZip(zipData);
    const entries = zip.getEntries();
    console.log(`📋 ZIP Contents (${entries.length} files/entries):`);
    for (const entry of entries) {
      console.log(`   - ${entry.entryName} (${entry.header.size} bytes)`);
      
      // Show first 200 bytes of objects to verify content
      if (entry.entryName.startsWith("objects/")) {
        const content = entry.getData().toString("utf8").substring(0, 200);
        console.log(`     Preview: ${content.substring(0, 100)}...`);
      }
    }
  } catch (err) {
    console.warn(`⚠️  Could not inspect ZIP with adm-zip: ${err.message}`);
  }
}

testZipCreation().catch((err) => {
  console.error(`❌ Error: ${err.message}`);
  process.exit(1);
});
