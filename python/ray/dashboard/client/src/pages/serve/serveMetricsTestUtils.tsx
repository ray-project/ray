import React, { PropsWithChildren } from "react";
import { GlobalContext, GlobalContextType } from "../../App";
import { STYLE_WRAPPER } from "../../util/test-utils";

const defaultDashboardUids: NonNullable<GlobalContextType["dashboardUids"]> = {
  default: "rayDefaultDashboard",
  serve: "rayServeDashboard",
  serveDeployment: "rayServeDeploymentDashboard",
  data: "rayDataDashboard",
};

export const defaultServeMetricsGlobalContext: GlobalContextType = {
  metricsContextLoaded: true,
  grafanaHost: "localhost:3000",
  grafanaOrgId: "1",
  grafanaClusterFilter: undefined,
  dashboardUids: defaultDashboardUids,
  prometheusHealth: true,
  sessionName: "session-name",
  nodeMap: {},
  nodeMapByIp: {},
  namespaceMap: {},
  dashboardDatasource: "Prometheus",
  serverTimeZone: undefined,
  currentTimeZone: undefined,
  themeMode: "light",
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  toggleTheme: () => {},
};

/**
 * Test wrapper factory: full {@link GlobalContextType} with optional overrides
 * (e.g. `grafanaClusterFilter`, disabled Grafana for "metrics unavailable" cases).
 */
export const createServeMetricsTestWrapper = (
  overrides: Partial<GlobalContextType> = {},
) => {
  const ServeMetricsTestContextWrapper = ({
    children,
  }: PropsWithChildren<{}>) => (
    <GlobalContext.Provider
      value={{ ...defaultServeMetricsGlobalContext, ...overrides }}
    >
      <STYLE_WRAPPER>{children}</STYLE_WRAPPER>
    </GlobalContext.Provider>
  );
  ServeMetricsTestContextWrapper.displayName = "ServeMetricsTestContextWrapper";
  return ServeMetricsTestContextWrapper;
};
