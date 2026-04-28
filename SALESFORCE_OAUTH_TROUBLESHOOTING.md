# Salesforce OAuth2 Client Credentials Troubleshooting

## Error: `unsupported_grant_type`

**Status Code:** `400 Bad Request`  
**Error Message:** `{"error":"unsupported_grant_type","error_description":"grant type not supported"}`

---

## What This Means

Your Salesforce Connected App is **NOT configured** to use the **Client Credentials OAuth2 flow**, which is required by the integration agent.

---

## Fix (Required in Salesforce)

### Step 1: Go to Salesforce Setup

1. Open Salesforce → **Setup** (top-right, gear icon)
2. Navigate to: **Apps** → **App Manager**
3. Search for your Connected App (e.g., "MYCOM Integration Agent")
4. Click **⋮ (three dots)** → **Edit**

### Step 2: Enable Client Credentials Flow

1. Scroll down to **API (Enable OAuth Settings)**
2. In the **OAuth 2.0 Authorized Flows** section:
   - ✓ Check **Client Credentials Flow** (enable it)
   
3. In **OAuth Scopes** section, add these scopes:
   - ✓ `full` (Full access to org)
   - ✓ `api` (Access to SOAP/REST APIs)
   
4. Click **Save**

### Step 3: Verify Your Credentials

1. Still in App Manager, click your Connected App name
2. Click **Manage Consumer Details** (or **View Consumer Details**)
3. Copy the **Consumer Key** → paste into `.env` as `SF_CLIENT_ID`
4. Copy the **Consumer Secret** → paste into `.env` as `SF_CLIENT_SECRET`
   - **Important:** Click "Reveal" if the secret is hidden
   - No extra spaces or quotes!

### Step 4: Test the Connection

Run the diagnostic tool:

```bash
npm run sf:debug-oauth
```

**Expected output:**
```
✓ SUCCESS! Token obtained:
  - access_token: 00D...
  - instance_url: https://your-domain.salesforce.com
  - token_type: Bearer
```

---

## If You Still Get `unsupported_grant_type`

Try these additional steps:

### Option A: Regenerate the Consumer Secret

1. Go back to App Manager → Your app → **Manage Consumer Details**
2. Click **Rotate Consumer Secret**
3. Copy the new secret into `.env` as `SF_CLIENT_SECRET`
4. Test again with `npm run sf:debug-oauth`

### Option B: Verify Salesforce Edition

Client Credentials flow is available in:
- ✓ Developer Edition
- ✓ Professional Edition (with API enabled)
- ✓ Enterprise Edition
- ✓ Unlimited Edition
- ✓ Salesforce Cloud editions

**Not available in:**
- ✗ Sandbox Preview
- ✗ Some special Salesforce editions

Check your org type in **Setup → System Overview**.

### Option C: Create a New Connected App

If the existing one is misconfigured:

1. Go to **Setup** → **Apps** → **App Manager**
2. Click **New Connected App** (top-right)
3. Fill in:
   - **Connected App Name:** `sf-onprem-integration`
   - **API Name:** `sf_onprem_integration` (auto-fills)
   - **Contact Email:** your@email.com
4. Enable **Enable OAuth Settings**
5. Set **Callback URL:** `http://localhost:3000/oauth/callback` (placeholder)
6. Select **OAuth Scopes:** Add `full` and `api`
7. Enable **Client Credentials Flow**
8. Save
9. Wait 2-3 minutes for changes to propagate
10. Use the new Consumer Key/Secret in `.env`

---

## Correct .env Format

```env
SF_LOGIN_URL=https://login.salesforce.com
SF_CLIENT_ID=3MVG1ZJl64l5BVh7s...
SF_CLIENT_SECRET=1234567890abcdef1234...
```

**Rules:**
- ✓ No quotes around values
- ✓ No extra spaces before/after `=`
- ✓ No trailing spaces
- ✓ Use `https://login.salesforce.com` for **production**
- ✓ Use `https://test.salesforce.com` for **sandbox**

---

## Debugging Tips

### Check What's in Your Environment

```bash
# Windows PowerShell
$env:SF_LOGIN_URL
$env:SF_CLIENT_ID
$env:SF_CLIENT_SECRET

# macOS/Linux
echo $SF_LOGIN_URL
echo $SF_CLIENT_ID
echo $SF_CLIENT_SECRET
```

### Verify .env File

1. Open `.env` in your text editor
2. Make sure no values have quotes or extra spaces:
   ```
   ✗ SF_CLIENT_ID="3MVG..."  (wrong - has quotes)
   ✓ SF_CLIENT_ID=3MVG...    (correct)
   
   ✗ SF_CLIENT_ID = 3MVG...  (wrong - has spaces)
   ✓ SF_CLIENT_ID=3MVG...    (correct)
   ```

### Run Diagnostic Tool

```bash
npm run sf:debug-oauth
```

This will:
- ✓ Validate your inputs
- ✓ Test the OAuth2 connection
- ✓ Provide specific error diagnosis
- ✓ Show next steps for your specific error

---

## Still Not Working?

### Collect Debug Info

Run this and share the output:

```bash
npm run sf:debug-oauth > debug-output.txt 2>&1
cat debug-output.txt
```

### Check Salesforce Setup Audit

1. **Setup** → **System Overview** → Your Salesforce Edition
2. **Setup** → **Apps** → **App Manager** → Your app → **View**
3. Verify **OAuth 2.0 Authorized Flows** shows:
   - ✓ Client Credentials Flow: **Allowed**
   - ✓ OAuth Scopes: includes `full`

### Wait for Propagation

After enabling Client Credentials flow, **wait 2-5 minutes** for Salesforce to propagate the change.

---

## Next Steps After OAuth Works

Once `npm run sf:debug-oauth` shows **SUCCESS**, run the full setup:

```bash
npm run init:installation -- --mode SAGE100 --activate
```

This will:
1. ✓ Test Salesforce connection
2. ✓ Deploy metadata (Custom Objects, Fields)
3. ✓ Create integration records
4. ✓ Setup SAGE100 database sync
5. ✓ Activate schedules

---

## References

- [Salesforce OAuth 2.0 Client Credentials Flow](https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_client_credentials.htm)
- [Connected Apps Setup](https://help.salesforce.com/s/articleView?id=sf.connected_app_overview.htm)
