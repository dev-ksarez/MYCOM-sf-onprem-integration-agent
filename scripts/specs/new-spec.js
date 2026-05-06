const fs = require("fs");
const path = require("path");

const repoRoot = path.resolve(__dirname, "..", "..");
const specsRoot = path.join(repoRoot, "docs", "specs");
const templatePath = path.join(specsRoot, "_template", "feature-spec.md");

function slugify(value) {
  return value
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .replace(/-{2,}/g, "-");
}

function formatDatePrefix(date) {
  return date.toISOString().slice(0, 10);
}

function main() {
  const rawTitle = process.argv.slice(2).join(" ").trim();
  if (!rawTitle) {
    console.error("Verwendung: npm run spec:new -- \"Kurzer Feature-Titel\"");
    process.exit(1);
  }

  if (!fs.existsSync(templatePath)) {
    console.error(`Template nicht gefunden: ${templatePath}`);
    process.exit(1);
  }

  const slug = slugify(rawTitle);
  if (!slug) {
    console.error("Der Titel ergibt keinen gueltigen Slug.");
    process.exit(1);
  }

  const specDirName = `${formatDatePrefix(new Date())}-${slug}`;
  const specDir = path.join(specsRoot, specDirName);
  const specPath = path.join(specDir, "spec.md");

  if (fs.existsSync(specDir)) {
    console.error(`Spec existiert bereits: ${specDir}`);
    process.exit(1);
  }

  const template = fs.readFileSync(templatePath, "utf8");
  const filled = template
    .replace(/\{\{TITLE\}\}/g, rawTitle)
    .replace(/\{\{SLUG\}\}/g, slug)
    .replace(/\{\{DATE\}\}/g, formatDatePrefix(new Date()));

  fs.mkdirSync(specDir, { recursive: true });
  fs.writeFileSync(specPath, filled, "utf8");

  console.log(`Spec erstellt: ${path.relative(repoRoot, specPath)}`);
  console.log("Naechster Schritt: Spec ausfuellen und danach npm run spec:validate ausfuehren.");
}

main();