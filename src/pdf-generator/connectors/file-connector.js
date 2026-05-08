const fs = require('fs');
const path = require('path');
const Handlebars = require('handlebars');

async function ensureDir(dir) {
  return fs.promises.mkdir(dir, { recursive: true });
}

/**
 * Save a buffer to a file backend using the provided connector config.
 * @param {Buffer} buffer
 * @param {object} context - rendering/context variables used for templating pathTemplate
 * @param {object} config - connector config.options
 */
async function save(buffer, context = {}, config = {}) {
  const basePath = config.basePath || 'artifacts/pdf';
  const pathTemplate = config.pathTemplate || '{{timestamp}}.pdf';

  // render path using Handlebars
  const tpl = Handlebars.compile(pathTemplate);
  const relPath = tpl(context);
  const fullPath = path.join(basePath, relPath);
  const dir = path.dirname(fullPath);

  await ensureDir(dir);

  await fs.promises.writeFile(fullPath, buffer);

  // set permissions if requested
  if (config.permissions) {
    try {
      await fs.promises.chmod(fullPath, parseInt(config.permissions, 8));
    } catch (e) {
      // ignore chmod errors on unsupported platforms
    }
  }

  const uri = `file://${fullPath}`;
  return {
    uri,
    path: fullPath,
    metadata: {
      size: buffer.length,
      createdAt: new Date().toISOString()
    }
  };
}

module.exports = { save };
