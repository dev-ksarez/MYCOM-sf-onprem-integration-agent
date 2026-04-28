import { CanonicalAccount } from "../../types/canonical-account";

export class MockAccountSource {
  public async getAccounts(): Promise<CanonicalAccount[]> {
    return [
      {
        externalKey: "ACC-1001",
        sourceId: "001XXXXXXXXXXXX001",
        name: "Acme GmbH",
        accountNumber: "1001",
        billingStreet: "Musterstraße 1",
        billingPostalCode: "10115",
        billingCity: "Berlin",
        billingCountry: "DE",
        phone: "+49 30 123456",
        website: "https://acme.example",
        lastModified: new Date().toISOString(),
        sourceSystem: "salesforce",
        targetSystem: "mock"
      },
      {
        externalKey: "ACC-1002",
        sourceId: "001XXXXXXXXXXXX002",
        name: "Globex AG",
        accountNumber: "1002",
        billingStreet: "Hauptstraße 10",
        billingPostalCode: "20095",
        billingCity: "Hamburg",
        billingCountry: "DE",
        phone: "+49 40 987654",
        website: "https://globex.example",
        lastModified: new Date().toISOString(),
        sourceSystem: "salesforce",
        targetSystem: "mock"
      }
    ];
  }
}