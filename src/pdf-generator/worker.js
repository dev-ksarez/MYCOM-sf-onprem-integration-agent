const fs = require('fs');
const path = require('path');
const Handlebars = require('handlebars');
const sanitizeHtml = require('sanitize-html');

let renderer = null;
try {
  renderer = require('./renderer');
} catch (e) {
  // renderer may not exist in prototype; fall back to mock
}

const fileConnector = require('./connectors/file-connector');

async function processJob(jobPath) {
  const job = JSON.parse(fs.readFileSync(jobPath, 'utf8'));
  const templatePath = job.template;
  const templateSrc = fs.readFileSync(templatePath, 'utf8');
  const tpl = Handlebars.compile(templateSrc);

  // Simulate a query result for prototype runs. Real worker will fetch data.
  const queryResult = { accountId: '001xx', name: 'Test Account' };

  const context = {
    title: job.title || 'PDF Report',
    createdAt: new Date().toISOString(),
    items: [{ name: 'Beispiel', value: 42 }],
    queryResult,
    templateName: path.basename(templatePath, path.extname(templatePath)),
    timestamp: new Date().toISOString().replace(/[:.]/g, '-'),
    instanceId: job.id || 'job-unknown'
  };

  let html = tpl(context);

  // sanitize rendered HTML to avoid unsafe content
  html = sanitizeHtml(html, {
    allowedTags: sanitizeHtml.defaults.allowedTags.concat(['img']),
    allowedAttributes: Object.assign({}, sanitizeHtml.defaults.allowedAttributes, { img: ['src', 'alt'] }),
    allowedSchemesByTag: Object.assign({}, sanitizeHtml.defaults.allowedSchemesByTag, { img: ['data', 'http', 'https'] })
  });

  let pdfBuffer;
  if (renderer && typeof renderer.renderHtmlToPdf === 'function') {
    // Prefer real renderer when available
    pdfBuffer = await renderer.renderHtmlToPdf(html, { format: 'A4' });
  } else {
    // Fallback mock PDF buffer for prototype/testing
    pdfBuffer = Buffer.from('%PDF-1.4\n%mock-pdf\n' + html);
  }

  // Choose connector; for sample job we use a local file connector
  const connectorConfig = job.connectorConfig || {
    basePath: 'tmp/artifacts',
    pathTemplate: job.outputFileNameTemplate || '{{instanceId}}/{{templateName}}-{{timestamp}}.pdf',
    permissions: '0644'
  };

  const saveResult = await fileConnector.save(pdfBuffer, context, connectorConfig);

  return { jobId: job.id || null, saveResult };
}

module.exports = { processJob };
