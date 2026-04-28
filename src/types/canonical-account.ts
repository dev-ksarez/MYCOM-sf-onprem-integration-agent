export interface CanonicalAccount {
  externalKey: string;
  sourceId: string;
  name: string;
  accountNumber?: string;
  billingStreet?: string;
  billingPostalCode?: string;
  billingCity?: string;
  billingCountry?: string;
  phone?: string;
  website?: string;
  lastModified: string;
  sourceSystem: "salesforce";
  targetSystem: string;
}