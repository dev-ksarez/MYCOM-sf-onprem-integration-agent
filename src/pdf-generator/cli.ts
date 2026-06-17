#!/usr/bin/env ts-node
import { renderFileToPdf } from './renderer';
import * as yargs from 'yargs';

async function main() {
  const argv = yargs
    .option('input', { type: 'string', demandOption: true, description: 'Path to input HTML file' })
    .option('output', { type: 'string', demandOption: true, description: 'Path to output PDF file' })
    .option('format', { type: 'string', default: 'A4' })
    .help()
    .parseSync();

  await renderFileToPdf(argv.input as string, argv.output as string, { format: argv.format as any });
  console.log('PDF generated:', argv.output);
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
