#!/usr/bin/env python3
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from pypdf import PdfReader


FIELD_RE = re.compile(r"^([A-Za-z_][A-Za-z0-9_äöüÄÖÜß]*)\s+([A-Za-z0-9]+(?:\([^)]+\))?)\s+(Yes|No)$")


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def extract_table_name(text: str) -> str:
    match = re.search(r"Tabelle:\s*([A-Za-z0-9_äöüÄÖÜß]+)", text)
    return match.group(1).strip() if match else ""


def parse_fields(lines):
    fields = []
    in_fields = False
    i = 0
    while i < len(lines):
        line = normalize_text(lines[i])
        if line == "Felder:":
            in_fields = True
            i += 1
            continue
        if in_fields and line in ("Indizes:", "Beziehungen:"):
            break
        if not in_fields:
            i += 1
            continue

        match = FIELD_RE.match(line)
        if not match:
            i += 1
            continue

        description_parts = []
        j = i + 1
        while j < len(lines):
            next_line = normalize_text(lines[j])
            if not next_line or next_line == "Standardwert:":
                break
            if FIELD_RE.match(next_line) or next_line in ("Indizes:", "Beziehungen:"):
                break
            if next_line.startswith("Datenbankdokume"):
                next_line = next_line.replace("Datenbankdokume", "", 1).strip()
            if next_line and not next_line.startswith("Seite "):
                description_parts.append(next_line)
            j += 1

        fields.append(
            {
                "name": match.group(1),
                "type": match.group(2),
                "required": match.group(3) == "Yes",
                "description": normalize_text(" ".join(description_parts)) or None,
            }
        )
        i = max(j, i + 1)

    return fields


def parse_primary_key(lines):
    for idx, raw_line in enumerate(lines):
        line = normalize_text(raw_line)
        if line != "PrimaryKey":
            continue
        fields = []
        j = idx + 1
        while j < len(lines):
            current = normalize_text(lines[j])
            if current == "Feld Aufsteigend":
                j += 1
                continue
            if current.startswith("Primärschlüssel:"):
                return fields
            parts = current.split()
            if len(parts) >= 2 and parts[-1] in ("No", "Yes"):
                fields.append(parts[0])
            j += 1
    return []


def main():
    source = Path(sys.argv[1] if len(sys.argv) > 1 else "artifacts/Datenbankdokumentation.pdf")
    target = Path(sys.argv[2] if len(sys.argv) > 2 else "artifacts/sage100-db-doc-index.json")
    reader = PdfReader(str(source))
    tables = {}

    for page_index, page in enumerate(reader.pages):
        text = page.extract_text() or ""
        table_name = extract_table_name(text)
        if not table_name:
            continue
        lines = [normalize_text(line) for line in text.splitlines()]
        entry = tables.setdefault(
            table_name,
            {
                "name": table_name,
                "pages": [],
                "fields": [],
                "primaryKey": [],
            },
        )
        entry["pages"].append(page_index + 1)

        existing_fields = {field["name"] for field in entry["fields"]}
        for field in parse_fields(lines):
            if field["name"] not in existing_fields:
                entry["fields"].append(field)
                existing_fields.add(field["name"])

        primary_key = parse_primary_key(lines)
        if primary_key:
            entry["primaryKey"] = primary_key

    document = {
        "sourceFile": str(source),
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "pageCount": len(reader.pages),
        "tableCount": len(tables),
        "tables": sorted(tables.values(), key=lambda item: item["name"].lower()),
    }

    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(document, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {len(tables)} tables to {target}")


if __name__ == "__main__":
    main()
