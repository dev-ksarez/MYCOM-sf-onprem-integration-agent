import express from 'express';
import bodyParser from 'body-parser';
import { renderHtmlToPdf } from './renderer';

const app = express();
app.use(bodyParser.json({ limit: '2mb' }));

app.post('/api/v1/pdf', async (req, res) => {
  try {
    const { format, content } = req.body as any;
    if (!content) return res.status(400).json({ error: 'content required' });
    const pdf = await renderHtmlToPdf(content, { format });
    res.setHeader('Content-Type', 'application/pdf');
    res.send(pdf);
  } catch (err: any) {
    console.error('render error', err);
    res.status(500).json({ error: 'render failed' });
  }
});

if (require.main === module) {
  const port = process.env.PORT || 3000;
  app.listen(Number(port), () => console.log(`pdf-generator http listening on ${port}`));
}
