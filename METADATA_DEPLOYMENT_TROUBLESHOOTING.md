# 🔧 QUICK FIX: Salesforce Custom Object Not Found Error

## The Problem
```
ERROR at Row:2:Column:10
sObject type 'MSD_Connector__c' is not supported. If you are attempting to use a custom object, 
be sure to append the '__c' after the entity name.
```

Or similar error mentioning:
- `MSD_Schedule__c`
- `MSD_Run__c`
- `MSD_Checkpoint__c`
- Other `MSD_*__c` objects

## What This Means

The **Salesforce custom objects weren't deployed** to your org. The `init:installation` script automatically deploys these, but something went wrong.

---

## The Fix

### Step 1: Check the init:installation Output

Run the setup again and look for the metadata deployment step:

```bash
npm run init:installation -- --mode SAGE100
```

Watch for this output:
```
📦 Deploying Salesforce metadata...
  Deploying ZIP (xx.x KB)...
  Deploy ID: 0Af...
  Status: Queued | Done: false | Errors: 0
  Status: InProgress | Done: false | Errors: 0
  Status: Done | Done: true | Errors: 0
  ✅ Metadata deployed successfully! (6 components)
```

### Step 2: If Metadata Deployment Failed

**Look for errors in the output like:**
```
❌ Metadata deployment had errors:
  [CustomObject] MSD_Connector__c: ...
  [CustomField] MSD_Connector__c.MSD_Active__c: ...
```

**Field-level errors are particularly important:**
If you see errors like:
```
[CustomField] MSD_Connector__c.MSD_Active__c: ...
[CustomField] MSD_Schedule__c.Active__c: ...
```

This means the custom object was created but individual fields failed to deploy. This causes the error:
```
No such column 'MSD_Active__c' on sobject of type MSD_Connector__c
```

**Common causes:**

1. **Missing Salesforce metadata files**
   - Verify folder exists: `salesforce/metadata/objects/`
   - Check files are present (should have `.object` files)

2. **Permissions issue**
   - Your Salesforce org might not allow metadata deployments
   - Check if your user has "Modify All Data" permission
   - Check if org allows API deployments

3. **Incomplete OAuth scope**
   - Connected App needs `full` scope or `metadata` scope
   - Go to Setup → Apps → App Manager → Your App → Edit
   - Add `full` scope if not present
   - Save and try again

### Step 3: Manual Verification

After `npm run init:installation` completes, verify objects exist:

1. Go to Salesforce → Setup → **Objects and Fields** → **Object Manager**
2. Search for `MSD_Connector__c`
3. Verify the object exists AND has these fields:
   - ✓ Name
   - ✓ MSD_Active__c (Checkbox)
   - ✓ MSD_ConnectorType__c (Text)
   - ✓ MSD_Parameters__c (Long Text)
   - And others...

4. If the object exists but fields are missing:
   - The metadata deployment partially succeeded
   - See **Field Deployment Errors** section below

---

## Field Deployment Errors

### Symptom

```
No such column 'MSD_Active__c' on sobject of type MSD_Connector__c
```

Or after `init:installation`:
```
❌ Metadata deployment had errors:
  [CustomField] MSD_Connector__c.MSD_Active__c: ...
```

### Causes and Fixes

**1. Permission Issue**
   - User needs "Modify All Data" permission
   - Go to Setup → Users → [Your User] → Edit
   - Check permission set: "Modify All Data"
   - Or check profile permissions for custom object field creation

**2. Organization Limit**
   - Org might have reached custom field limit
   - Developer Edition: 500 custom fields per org
   - Check Setup → System Overview → Custom Object Fields Used / Limit

**3. Metadata XML Configuration**
   - Fields might have invalid XML structure
   - Try deploying in Salesforce directly via Setup → Deploy
   - Upload the `salesforce/metadata/` folder as a ZIP
   - Look for specific XML validation errors

**4. Scope Issue in Connected App**
   - Ensure OAuth scope includes "modify metadata"
   - Go to Setup → Apps → App Manager → Your App → Edit
   - Add `full` or `metadata` scope
   - Save and wait 2-5 minutes
   - Regenerate and copy new Client Secret
   - Retry deployment

---

## Troubleshooting

### Enable More Logging

Edit your `.env` and add:
```env
DEBUG=*
```

Then run:
```bash
npm run init:installation -- --mode SAGE100
```

This will show detailed deployment information.

### Check Salesforce Deployment History

1. Go to Setup → **Deployment Status**
2. Look for recent deployments
3. If failed, click to see error details

### Verify Connected App Scopes

1. Setup → **Apps** → **App Manager**
2. Find your Connected App → **Edit**
3. In **OAuth Scopes**, check you have:
   - ✓ `full` (Full access to org)
   - ✓ `api` (or `metadata` scope)
4. **Save** if you added scopes
5. **Wait 2-5 minutes** for Salesforce to apply
6. Run `init:installation` again

### Network/Firewall Issues

If deployment times out or shows network errors:
- Check your network allows HTTPS to Salesforce
- Check your firewall isn't blocking API calls
- Try from a different network if available

---

## If All Else Fails

### Collect Debug Information

```bash
npm run init:installation -- --mode SAGE100 > init-installation.log 2>&1
cat init-installation.log
```

Share the log file with support. Include:
- Full init:installation output
- Your Salesforce org type (Developer, Professional, Enterprise, etc.)
- Your region (US, EU, etc.)
- Connected App configuration screenshot (with secrets redacted)

### Manual Workaround (Not Recommended)

If you have TypeScript installed locally:

```bash
npx ts-node scripts/salesforce/deploy-metadata.ts
```

**Note:** This requires `SF_USERNAME` and `SF_PASSWORD` instead of OAuth. Less secure.

---

## Prevention

- Always ensure OAuth scopes include `full` or `metadata` before running `init:installation`
- Keep the `salesforce/metadata/` folder in your package
- Run `init:installation` early in setup, before other operations
