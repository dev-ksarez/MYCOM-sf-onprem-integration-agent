const fs = require('fs');
const path = require('path');

/**
 * Prototype stub for saving PDF to Salesforce.
 * This implementation simulates uploading by writing a metadata file to tmp/sf-mock.
 * Real implementation should call Salesforce REST API (ContentVersion) with OAuth creds.
 */
async function save(buffer, context = {}, config = {}) {
  const mockDir = path.join('tmp', 'sf-mock');
  await fs.promises.mkdir(mockDir, { recursive: true });

  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const filename = `sf-${config.targetObject || 'Record'}-${timestamp}.pdf`;
  const pdfPath = path.join(mockDir, filename);
  await fs.promises.writeFile(pdfPath, buffer);

  const meta = {
    storedAt: pdfPath,
    targetObject: config.targetObject || null,
    linkMethod: config.linkMethod || 'ContentVersion',
    createdAt: new Date().toISOString(),
    simulatedId: `SF${Math.floor(Math.random()*1000000)}`
  };

  const metaPath = pdfPath + '.json';
  await fs.promises.writeFile(metaPath, JSON.stringify(meta, null, 2));

  return {
    salesforceId: meta.simulatedId,
    metadata: meta
  };
}

module.exports = { save };
