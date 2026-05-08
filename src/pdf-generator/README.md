Playwright PDF Generator — Prototype

Quickstart

1. Install dependencies:

```bash
npm install --save playwright express body-parser yargs
# optionally: npm i -D ts-node typescript @types/express @types/node
```

2. Run CLI:

```bash
npx ts-node src/pdf-generator/cli.ts --input examples/sample.html --output out.pdf
```

3. Run HTTP server:

```bash
npx ts-node src/pdf-generator/http.ts
curl -X POST http://localhost:3000/api/v1/pdf -H 'Content-Type: application/json' -d '{"content":"<h1>Hello</h1>"}' --output out.pdf
```

Notes
- This is a minimal prototype. In production, run Playwright browsers in a pool, sanitize HTML, and secure the HTTP endpoint.
