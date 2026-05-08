Example Handlebars template for the PDF prototype.

Usage examples:

Render to stdout (compile + print HTML):

```bash
node -e "const fs=require('fs');const Handlebars=require('handlebars');const t=Handlebars.compile(fs.readFileSync('src/pdf-generator/templates/example.hbs','utf8'));console.log(t({title:'Beispiel', createdAt: new Date().toISOString(), items:[{name:'A',value:1},{name:'B',value:2}]}));"
```

Send rendered HTML to the prototype HTTP endpoint:

```bash
node -e "const fs=require('fs');const Handlebars=require('handlebars');const t=Handlebars.compile(fs.readFileSync('src/pdf-generator/templates/example.hbs','utf8'));const html=t({title:'Beispiel', createdAt: new Date().toISOString(), items:[{name:'A',value:1},{name:'B',value:2}]});require('child_process').execSync('curl -s -X POST http://localhost:3000/api/v1/pdf -H \"Content-Type: text/html\" --data-binary @-', {input: html});"
```

Notes:
- Templates are HTML-first and Handlebars-escaped by default.
- Do not accept untrusted templates without validation and review.
