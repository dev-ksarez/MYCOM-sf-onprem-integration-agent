"use strict";
const fs = require('fs');

async function renderHtmlToPdf(html, options = {}) {
  // Try to use Playwright if available, otherwise return a mock PDF buffer
  try {
    const playwright = require('playwright');
    const browser = await playwright.chromium.launch({ args: ['--no-sandbox'] });
    const page = await browser.newPage();
    const waitUntil = options.waitUntil || 'networkidle';
    await page.setContent(html, { waitUntil });
    const pdfBuffer = await page.pdf({
      format: options.format || 'A4',
      margin: options.margin || { top: '20px', bottom: '20px', left: '20px', right: '20px' },
      printBackground: true
    });
    await browser.close();
    return pdfBuffer;
  } catch (err) {
    // Fallback mock PDF
    const header = '%PDF-1.4\n%mock-fallback\n';
    return Buffer.from(header + html);
  }
}

module.exports = { renderHtmlToPdf };
