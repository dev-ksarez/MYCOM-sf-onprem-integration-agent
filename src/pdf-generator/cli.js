#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const Handlebars = require('handlebars');
const { renderHtmlToPdf } = require('./renderer');

function loadJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

async function main() {
  const argv = process.argv.slice(2);
  if (argv.length === 0) {
    console.error('Usage: cli.js --template template.hbs --data data.json --out out.pdf');
    process.exit(2);
  }

  let templatePath = null;
  let dataPath = null;
  let outPath = 'out.pdf';
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--template') templatePath = argv[++i];
    else if (argv[i] === '--data') dataPath = argv[++i];
    else if (argv[i] === '--out') outPath = argv[++i];
  }

  if (!templatePath) {
    console.error('Missing --template');
    process.exit(2);
  }

  const tplSrc = fs.readFileSync(templatePath, 'utf8');
  const tpl = Handlebars.compile(tplSrc);
  const data = dataPath ? loadJson(dataPath) : {};
  const html = tpl(data);

  const pdf = await renderHtmlToPdf(html, { format: 'A4' });
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, pdf);
  console.log('Wrote', outPath);
}

main().catch(err => { console.error(err); process.exit(1); });
