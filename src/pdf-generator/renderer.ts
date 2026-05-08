import { chromium } from 'playwright';

export type RenderOptions = {
  format?: 'A4' | 'Letter' | string;
  landscape?: boolean;
  margin?: { top?: string; bottom?: string; left?: string; right?: string };
};

export async function renderHtmlToPdf(html: string, options: RenderOptions = {}) {
  const browser = await chromium.launch({ args: ['--no-sandbox'] });
  const page = await browser.newPage();
  await page.setContent(html, { waitUntil: 'networkidle' });
  const pdf = await page.pdf({
    format: options.format ?? 'A4',
    landscape: options.landscape ?? false,
    margin: options.margin as any,
  });
  await browser.close();
  return pdf;
}

export async function renderFileToPdf(inputPath: string, outputPath: string, options: RenderOptions = {}) {
  const fs = await import('fs');
  const html = fs.readFileSync(inputPath, 'utf8');
  const pdf = await renderHtmlToPdf(html, options);
  fs.writeFileSync(outputPath, pdf);
}
