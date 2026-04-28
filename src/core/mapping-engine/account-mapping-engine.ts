

import {
  FieldMappingData,
  SalesforceAccountRecord
} from "../../clients/salesforce/salesforce-client";
import { CanonicalAccount } from "../../types/canonical-account";

const SUPPORTED_CANONICAL_FIELDS = new Set([
  "externalKey",
  "sourceId",
  "name",
  "accountNumber",
  "billingStreet",
  "billingPostalCode",
  "billingCity",
  "billingCountry",
  "phone",
  "website",
  "lastModified"
]);

function getSourceValue(record: SalesforceAccountRecord, sourceFieldApi: string): string | undefined {
  const value = record[sourceFieldApi as keyof SalesforceAccountRecord];

  if (value === null || value === undefined) {
    return undefined;
  }

  return String(value);
}

export class AccountMappingEngine {
  public mapRecord(
    record: SalesforceAccountRecord,
    fieldMappings: FieldMappingData[],
    targetSystem: string
  ): CanonicalAccount {
    const mappedValues: Partial<CanonicalAccount> = {
      sourceSystem: "salesforce",
      targetSystem
    };

    for (const fieldMapping of fieldMappings) {
      if (!fieldMapping.active) {
        continue;
      }

      if (!fieldMapping.canonicalField || !fieldMapping.sourceFieldApi) {
        continue;
      }

      if (!SUPPORTED_CANONICAL_FIELDS.has(fieldMapping.canonicalField)) {
        continue;
      }

      const value = getSourceValue(record, fieldMapping.sourceFieldApi);

      if (value === undefined) {
        continue;
      }

      mappedValues[fieldMapping.canonicalField as keyof CanonicalAccount] = value as never;
    }

    if (!mappedValues.externalKey) {
      throw new Error(`Mapped account is missing required field externalKey for source record ${record.Id}`);
    }

    if (!mappedValues.sourceId) {
      throw new Error(`Mapped account is missing required field sourceId for source record ${record.Id}`);
    }

    if (!mappedValues.name) {
      throw new Error(`Mapped account is missing required field name for source record ${record.Id}`);
    }

    if (!mappedValues.lastModified) {
      throw new Error(`Mapped account is missing required field lastModified for source record ${record.Id}`);
    }

    return mappedValues as CanonicalAccount;
  }
}