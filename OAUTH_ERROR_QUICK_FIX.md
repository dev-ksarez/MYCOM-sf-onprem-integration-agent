# 🔧 QUICK FIX: Salesforce OAuth Error

## The Problem
```
Error: Salesforce token request failed: 400 {"error":"unsupported_grant_type",...}
```

## The Solution (5-minute fix)

### 1️⃣ Check Your Salesforce Connected App

You need to **enable Client Credentials Flow**. Do this:

1. Log into **Salesforce** → Click gear icon → **Setup**
2. Go to: **Apps** → **App Manager**
3. Find your Connected App (e.g., "MYCOM Integration Agent")
4. Click **⋮ (menu)** → **Edit**
5. Scroll to **API (Enable OAuth Settings)**
6. ✅ Check the box: **"Client Credentials Flow"**
7. Make sure **OAuth Scopes** includes: `full` and `api`
8. Click **Save**
9. ⏳ **Wait 2-5 minutes** for Salesforce to apply the change

### 2️⃣ Test the Connection

After enabling Client Credentials Flow in Salesforce, test with:

```bash
npm run sf:debug-oauth
```

Expected output:
```
✓ SUCCESS! Token obtained
```

### 3️⃣ Then Run Installation

Once the test passes, run the full setup:

```bash
npm run init:installation -- --mode SAGE100 --activate
```

---

## Still Not Working?

### Step A: Verify Credentials in `.env`
Open your `.env` file and check:
- No quotes around values
- No extra spaces
- Correct format:
```env
SF_LOGIN_URL=https://login.salesforce.com
SF_CLIENT_ID=3MVG...
SF_CLIENT_SECRET=...
```

### Step B: Run Detailed Diagnostics
```bash
npm run sf:debug-oauth
```

This will:
- ✓ Validate your inputs
- ✓ Test OAuth2 connection
- ✓ Show exactly what's wrong

### Step C: Read Full Guide
For more detailed troubleshooting:
```bash
cat SALESFORCE_OAUTH_TROUBLESHOOTING.md
```

---

## Common Issues

| Problem | Solution |
|---------|----------|
| Still getting `unsupported_grant_type` | Wait 5 min for Salesforce to apply change, then try again |
| Client ID/Secret not recognized | Regenerate from Salesforce App Manager |
| Wrong Salesforce org | Make sure credentials are from the right org (prod/sandbox) |
| Firewall blocking | Check if your network allows outbound HTTPS to salesforce.com |

---

## Support

If you still can't fix it:
1. Run: `npm run sf:debug-oauth > debug.txt`
2. Share the output from `debug.txt`
3. Share your Salesforce edition/type from Setup → System Overview
