import { MappingPicklistEntry, MappingTargetType, MappingTransformType } from "./mapping-definition-types";

export interface SalesforceMappingSourceField {
  name: string;
  type?: string;
}

export interface SalesforceMappingTargetField {
  name: string;
  label?: string;
  type?: string;
  isExternalId?: boolean;
}

export interface SalesforceGeneratedMappingRule {
  sourceField: string;
  sourceType: string;
  targetField: string;
  targetFieldLabel?: string;
  targetFieldType?: string;
  targetType: MappingTargetType;
  transformFunction: MappingTransformType;
  transformExpression?: string;
  lookupEnabled: boolean;
  lookupObject?: string;
  lookupField?: string;
  picklistMappings: MappingPicklistEntry[];
}

export interface SalesforceMappingGeneratorInput {
  sourceFields: SalesforceMappingSourceField[];
  targetFields: SalesforceMappingTargetField[];
  targetObjectApiName?: string;
  profile?: SalesforceMappingProfile;
}

export type SalesforceMappingProfile =
  | "standard"
  | "salesforce-product"
  | "salesforce-pricebook";

function normalizeKey(value: unknown): string {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

function toTargetType(fieldType: string | undefined): MappingTargetType {
  switch (String(fieldType || "").trim().toLowerCase()) {
    case "int":
    case "integer":
      return "integer";
    case "double":
    case "currency":
    case "percent":
    case "number":
      return "number";
    case "boolean":
      return "boolean";
    case "date":
    case "datetime":
      return "datetime";
    default:
      return "string";
  }
}

function inferProfile(targetObjectApiName: string, requestedProfile?: SalesforceMappingProfile): SalesforceMappingProfile {
  if (requestedProfile) {
    return requestedProfile;
  }

  if (targetObjectApiName === "PricebookEntry") {
    return "salesforce-pricebook";
  }

  if (targetObjectApiName === "Product2") {
    return "salesforce-product";
  }

  return "standard";
}

function buildAliasMap(
  targetObjectApiName: string,
  profile: SalesforceMappingProfile
): Map<string, { targetField: string; lookupObject?: string; lookupField?: string }> {
  const aliases = new Map<string, { targetField: string; lookupObject?: string; lookupField?: string }>();
  const add = (keys: string[], targetField: string, lookupObject?: string, lookupField?: string) => {
    for (const key of keys) {
      aliases.set(normalizeKey(key), { targetField, lookupObject, lookupField });
    }
  };

  add(["email", "emailaddress", "mail"], "Email");
  add(["firstname", "vorname", "givenname"], "FirstName");
  add(["lastname", "surname", "nachname", "familyname"], "LastName");
  add(["phone", "telefon", "telephone"], "Phone");
  add(["mobile", "mobilephone", "handy", "mobiltelefon"], "MobilePhone");
  add(["website", "url", "webseite"], "Website");
  add(["accountnumber", "customernumber", "kundennummer", "debitorennummer"], "AccountNumber");
  add(["productcode", "sku", "itemnumber", "articlenumber", "artikelnummer", "artnr"], "ProductCode");
  add(["apiname", "api", "code", "key", "externalkey", "externalid", "gruppe", "group", "groupcode", "artikelgruppe"], "ApiName");
  add(["label", "name", "bezeichnung", "description", "groupname", "grouplabel", "artikelgruppenbezeichnung"], "Label");

  if (profile === "salesforce-product" || profile === "salesforce-pricebook") {
    add(["name", "productname", "produktname", "bezeichnung", "artikelbezeichnung"], "Name");
    add(["description", "beschreibung", "produktbeschreibung"], "Description");
    add(["family", "productfamily", "produktfamilie", "warengruppe"], "Family");
    add(["active", "isactive", "aktiv"], "IsActive");
    add(["stockkeepingunit", "sku", "productcode", "itemnumber", "articlenumber", "artikelnummer", "artnr"], "ProductCode");
  }

  if (profile === "salesforce-pricebook" || targetObjectApiName === "PricebookEntry") {
    add(["price", "unitprice", "listprice", "salesprice", "verkaufspreis", "einzelpreis", "netprice", "grossprice"], "UnitPrice");
    add(["active", "isactive", "aktiv"], "IsActive");
    add(["usestandardprice", "standardprice", "standardpreis"], "UseStandardPrice");
    add(["currency", "currencyisocode", "waehrung", "währung"], "CurrencyIsoCode");
    add(["pricebook", "pricebookid", "pricebook2", "pricebook2id", "preisliste"], "Pricebook2Id");
    add(
      ["productcode", "sku", "itemnumber", "articlenumber", "artikelnummer", "artnr", "produktcode", "produkt", "product"],
      "Product2Id",
      "Product2",
      "ProductCode"
    );
  }

  const state: { selectedObject?: string } = {}; // Lokale Variable für den Zustand mit optionaler Eigenschaft
  const selectedObject = state.selectedObject || null; // Sicherstellen, dass selectedObject definiert ist

  if (!selectedObject) {
    add(["customer", "kunde", "client"], "Customer");
  }

  return aliases;
}

function buildTargetIndex(targetFields: SalesforceMappingTargetField[]): Map<string, SalesforceMappingTargetField> {
  const index = new Map<string, SalesforceMappingTargetField>();
  for (const field of targetFields) {
    const apiName = String(field?.name || "").trim();
    const label = String(field?.label || "").trim();
    if (apiName) {
      index.set(normalizeKey(apiName), field);
    }
    if (label) {
      index.set(normalizeKey(label), field);
    }
  }
  return index;
}

export function generateSalesforceMappingRules(
  input: SalesforceMappingGeneratorInput
): SalesforceGeneratedMappingRule[] {
  const targetObjectApiName = String(input.targetObjectApiName || "").trim();
  const targetIndex = buildTargetIndex(Array.isArray(input.targetFields) ? input.targetFields : []);
  const profile = inferProfile(targetObjectApiName, input.profile);
  const aliases = buildAliasMap(targetObjectApiName, profile);
  const generated: SalesforceGeneratedMappingRule[] = [];
  const seenTargets = new Set<string>();

  for (const sourceField of Array.isArray(input.sourceFields) ? input.sourceFields : []) {
    const sourceName = String(sourceField?.name || "").trim();
    if (!sourceName) {
      continue;
    }

    const sourceKey = normalizeKey(sourceName);
    if (!sourceKey) {
      continue;
    }

    let target = targetIndex.get(sourceKey);
    let alias = aliases.get(sourceKey);

    if (!target && alias?.targetField) {
      target = targetIndex.get(normalizeKey(alias.targetField));
    }

    if (!target?.name) {
      continue;
    }

    const targetName = String(target.name).trim();
    if (!targetName || seenTargets.has(targetName.toLowerCase())) {
      continue;
    }

    generated.push({
      sourceField: sourceName,
      sourceType: String(sourceField?.type || "string"),
      targetField: targetName,
      targetFieldLabel: String(target.label || targetName).trim() || targetName,
      targetFieldType: String(target.type || "").trim(),
      targetType: toTargetType(target.type),
      transformFunction: "NONE",
      lookupEnabled: Boolean(alias?.lookupObject && alias?.lookupField),
      lookupObject: alias?.lookupObject,
      lookupField: alias?.lookupField,
      picklistMappings: []
    });
    seenTargets.add(targetName.toLowerCase());
  }

  return generated;
}
