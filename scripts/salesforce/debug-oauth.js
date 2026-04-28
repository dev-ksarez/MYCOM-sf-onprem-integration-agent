#!/usr/bin/env node

/**
 * Salesforce OAuth2 Diagnostic Tool
 * Tests Client Credentials flow and provides detailed debugging info
 */

const fs = require("fs");
const path = require("path");
const readline = require("readline/promises");
const { stdin: input, stdout: output } = require("process");
const dotenv = require("dotenv");

dotenv.config();

const rl = readline.createInterface({ input, output });

async function askQuestion(label, defaultValue = "") {
  const suffix = defaultValue ? ` [${defaultValue}]` : "";
  const answer = (await rl.question(`${label}${suffix}: `)).trim();
  return answer || defaultValue;
}

function validateUrl(url) {
  try {
    new URL(url);
    return true;
  } catch {
    return false;
  }
}

async function main() {
  console.log("\n========================================");
  console.log("Salesforce OAuth2 Diagnostic Tool");
  console.log("========================================\n");

  // Get credentials
  let loginUrl = process.env.SF_LOGIN_URL || "";
  let clientId = process.env.SF_CLIENT_ID || "";
  let clientSecret = process.env.SF_CLIENT_SECRET || "";

  console.log("Current environment:");
  console.log(`  SF_LOGIN_URL: ${loginUrl ? "✓ Set" : "✗ Not set"}`);
  console.log(`  SF_CLIENT_ID: ${clientId ? `✓ Set (${clientId.length} chars)` : "✗ Not set"}`);
  console.log(`  SF_CLIENT_SECRET: ${clientSecret ? `✓ Set (${clientSecret.length} chars)` : "✗ Not set"}`);
  console.log();

  const interactive = await askQuestion("Use interactive mode? (y/n)", "y");
  if (interactive.toLowerCase() !== "n") {
    console.log("\nEnter your Salesforce credentials:");
    loginUrl = await askQuestion("Salesforce Login URL", loginUrl || "https://login.salesforce.com");
    clientId = await askQuestion("Client ID", clientId);
    clientSecret = await askQuestion("Client Secret", clientSecret);
  }

  console.log("\n========================================");
  console.log("VALIDATION");
  console.log("========================================\n");

  // Validate inputs
  const errors = [];

  if (!loginUrl) {
    errors.push("❌ SF_LOGIN_URL is required");
  } else if (!validateUrl(loginUrl)) {
    errors.push(`❌ SF_LOGIN_URL is invalid: ${loginUrl}`);
  } else {
    console.log(`✓ Login URL is valid: ${loginUrl}`);
  }

  if (!clientId) {
    errors.push("❌ SF_CLIENT_ID is required");
  } else if (clientId.length < 10) {
    errors.push(`❌ SF_CLIENT_ID looks too short (${clientId.length} chars)`);
  } else {
    console.log(`✓ Client ID format looks OK (${clientId.length} chars)`);
  }

  if (!clientSecret) {
    errors.push("❌ SF_CLIENT_SECRET is required");
  } else {
    console.log(`✓ Client Secret provided (${clientSecret.length} chars)`);
  }

  if (errors.length > 0) {
    console.log("\n" + errors.join("\n"));
    await rl.close();
    process.exit(1);
  }

  console.log("\n========================================");
  console.log("TESTING OAUTH2 TOKEN REQUEST");
  console.log("========================================\n");

  const tokenUrl = `${loginUrl.replace(/\/$/, "")}/services/oauth2/token`;
  console.log(`Token URL: ${tokenUrl}\n`);

  try {
    const body = new URLSearchParams({
      grant_type: "client_credentials",
      client_id: clientId,
      client_secret: clientSecret,
    });

    console.log("Sending request...");
    const response = await fetch(tokenUrl, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body,
    });

    const responseText = await response.text();
    let responseData;
    try {
      responseData = JSON.parse(responseText);
    } catch {
      responseData = responseText;
    }

    console.log(`Status: ${response.status} ${response.statusText}\n`);

    if (response.ok) {
      console.log("✓ SUCCESS! Token obtained:");
      if (typeof responseData === "object") {
        console.log(`  - access_token: ${responseData.access_token ? responseData.access_token.substring(0, 20) + "..." : "MISSING"}`);
        console.log(`  - instance_url: ${responseData.instance_url || "MISSING"}`);
        console.log(`  - token_type: ${responseData.token_type || "MISSING"}`);
      }
    } else {
      console.log("✗ TOKEN REQUEST FAILED\n");
      console.log("Response:", JSON.stringify(responseData, null, 2));

      if (typeof responseData === "object") {
        const error = responseData.error || "";
        const desc = responseData.error_description || "";

        console.log("\n🔍 Diagnosis:");
        switch (error) {
          case "unsupported_grant_type":
            console.log("  Your Salesforce Connected App is NOT configured for:");
            console.log("  'OAuth 2.0 Client Credentials Flow'");
            console.log("\n  Fix:");
            console.log("    1. Go to Salesforce Setup → Apps → App Manager");
            console.log("    2. Find your Connected App and click Edit");
            console.log("    3. In 'API (Enable OAuth Settings)':");
            console.log("       - Check: 'Client Credentials Flow' (enable it)");
            console.log("       - Add scope: 'Full access (full)' if not present");
            console.log("    4. Save and retry");
            break;

          case "invalid_client_id":
            console.log("  The Client ID does not match any Connected App");
            console.log("\n  Fix:");
            console.log("    1. Double-check SF_CLIENT_ID is copied correctly");
            console.log("    2. Make sure it's from the right Salesforce org");
            console.log("    3. Paste from Salesforce Setup → Apps → App Manager → [Your App]");
            break;

          case "invalid_client":
            console.log("  Client ID or Secret is invalid");
            console.log("\n  Fix:");
            console.log("    1. Verify both SF_CLIENT_ID and SF_CLIENT_SECRET");
            console.log("    2. They should have no extra spaces");
            console.log("    3. Regenerate the secret in Salesforce if needed");
            break;

          default:
            console.log(`  Error: ${error}`);
            console.log(`  Description: ${desc}`);
        }
      }
    }
  } catch (err) {
    console.log("✗ NETWORK ERROR\n");
    console.log(err.message);
    console.log("\nPossible causes:");
    console.log("  - Firewall/network blocking connection to Salesforce");
    console.log("  - Invalid Salesforce URL (check SF_LOGIN_URL)");
    console.log("  - Salesforce service temporarily down");
  }

  console.log("\n========================================");
  console.log("CONFIGURATION CHECKLIST");
  console.log("========================================\n");
  console.log("Before running init:installation, verify:");
  console.log("  ☐ Salesforce Connected App created");
  console.log("  ☐ OAuth 2.0 'Client Credentials Flow' is ENABLED");
  console.log("  ☐ Scopes include 'Full access (full)' or similar");
  console.log("  ☐ Client ID & Secret are fresh (regenerate if very old)");
  console.log("  ☐ SF_LOGIN_URL matches your org (prod/sandbox)");
  console.log("  ☐ No extra spaces in credentials (.env file)");
  console.log("");

  await rl.close();
}

main().catch(console.error);
