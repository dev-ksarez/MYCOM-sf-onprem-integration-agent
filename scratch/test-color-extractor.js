const sharp = require('sharp');
const path = require('path');

const imgPath = '/Users/karstensarez/Documents/Projekte/sf-onprem-integration-agent/data/custom-logo-annaburger-sage100.png';

async function analyzeLogo() {
  try {
    const image = sharp(imgPath);
    const { data, info } = await image
      .resize(50, 50, { fit: 'fill' })
      .raw()
      .toBuffer({ resolveWithObject: true });

    const colorCounts = {};
    let transparentCount = 0;
    let grayscaleCount = 0;

    for (let i = 0; i < data.length; i += 4) {
      const r = data[i];
      const g = data[i+1];
      const b = data[i+2];
      const a = data[i+3];

      if (a < 120) {
        transparentCount++;
        continue;
      }

      const maxVal = Math.max(r, g, b);
      const minVal = Math.min(r, g, b);
      if (maxVal - minVal < 25) {
        if (maxVal > 220 || maxVal < 35) {
          grayscaleCount++;
          continue;
        }
      }

      const qr = Math.round(r / 8) * 8;
      const qg = Math.round(g / 8) * 8;
      const qb = Math.round(b / 8) * 8;
      const key = `${qr},${qg},${qb}`;

      colorCounts[key] = (colorCounts[key] || 0) + 1;
    }

    console.log('Image dimensions:', info.width, 'x', info.height);
    console.log('Total pixels:', info.width * info.height);
    console.log('Transparent pixels skipped:', transparentCount);
    console.log('Grayscale pixels skipped:', grayscaleCount);

    const sortedColors = Object.entries(colorCounts)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10);

    console.log('\nTop 10 dominant colors found:');
    sortedColors.forEach(([color, count]) => {
      const [r, g, b] = color.split(',').map(Number);
      const hex = '#' + [r, g, b].map(c => {
        const hex = c.toString(16);
        return hex.length === 1 ? '0' + hex : hex;
      }).join('');
      console.log(`- RGB(${color}) Hex(${hex}): ${count} pixels`);
    });

  } catch (err) {
    console.error('Error analyzing image:', err);
  }
}

analyzeLogo();
