import { ConnectorConfig } from "../../clients/salesforce/salesforce-client";
import { MockConnector } from "../../connectors/mock/mock-connector";
import { TargetConnector } from "../../types/target-connector";
import { MssqlConnector } from "../../connectors/mssql/mssql-connector";
import { OracleConnector } from "../../connectors/oracle/oracle-connector";
import { FileMakerConnector } from "../../connectors/filemaker/filemaker-connector";
import { SalesforceConnector } from "../../connectors/salesforce/salesforce-connector";

interface CacheEntry {
  connector: TargetConnector;
  configJson: string;
}

export class ConnectorRegistry {
  private static readonly connectorCache = new Map<string, CacheEntry>();

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

    const configJson = JSON.stringify({
      connectorType: config.connectorType,
      targetSystem: config.targetSystem,
      parameters: config.parameters,
      secretKey: config.secretKey,
      timeoutMs: config.timeoutMs,
      active: config.active
    });

    const cached = ConnectorRegistry.connectorCache.get(config.id);
    if (cached && cached.configJson === configJson) {
      return cached.connector;
    }

    // Configuration has changed or isn't cached yet.
    // If a cached connector exists, release its connection pool.
    if (cached) {
      if (cached.connector.close) {
        cached.connector.close().catch(() => {});
      }
      ConnectorRegistry.connectorCache.delete(config.id);
    }

    let connector: TargetConnector;
    switch (config.connectorType.toLowerCase()) {
      case "mock":
        connector = new MockConnector();
        break;
      case "mssql":
        connector = new MssqlConnector(config);
        break;
      case "oracle":
        connector = new OracleConnector(config);
        break;
      case "filemaker":
      case "filemaker_data_api":
        connector = new FileMakerConnector(config);
        break;
      case "salesforce":
      case "salesforce_org":
      case "salesforce_connector":
        connector = new SalesforceConnector(config);
        break;
      default:
        throw new Error(`No connector registered for connector type: ${config.connectorType}`);
    }

    ConnectorRegistry.connectorCache.set(config.id, { connector, configJson });
    return connector;
  }
}
