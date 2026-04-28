import { ConnectorConfig } from "../../clients/salesforce/salesforce-client";
import { MockConnector } from "../../connectors/mock/mock-connector";
import { TargetConnector } from "../../types/target-connector";
import { MssqlConnector } from "../../connectors/mssql/mssql-connector";

export class ConnectorRegistry {
  public getConnector(targetSystem: string): TargetConnector {
    switch (targetSystem.toLowerCase()) {
      case "mock":
        return new MockConnector();
      default:
        throw new Error(`No connector registered for target system: ${targetSystem}`);
    }
  }

  public getConnectorByConfig(config: ConnectorConfig): TargetConnector {
    if (!config.active) {
      throw new Error(`Connector is inactive: ${config.name}`);
    }

    switch (config.connectorType.toLowerCase()) {
      case "mock":
        return new MockConnector();
      case "mssql":
        return new MssqlConnector(config);
      default:
        throw new Error(`No connector registered for connector type: ${config.connectorType}`);
    }
  }
}