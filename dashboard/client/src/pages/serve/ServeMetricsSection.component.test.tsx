import { render, screen, waitFor } from "@testing-library/react";
import React, { PropsWithChildren } from "react";
import { GlobalContext } from "../../App";
import { ServeMetricsSection } from "./ServeMetricsSection";

const Wrapper = ({ children }: PropsWithChildren<{}>) => {
  return (
    <GlobalContext.Provider
      value={{
        metricsContextLoaded: true,
        grafanaHost: "localhost:3000",
        dashboardUids: {
          default: "rayDefaultDashboard",
          serve: "rayServeDashboard",
          serveDeployment: "rayServeDeploymentDashboard",
          data: "rayDataDashboard",
        },
        prometheusHealth: true,
        sessionName: "session-name",
        nodeMap: {},
        nodeMapByIp: {},
        namespaceMap: {},
        dashboardDatasource: "Prometheus",
      }}
    >
      {children}
    </GlobalContext.Provider>
  );
};

const MetricsDisabledWrapper = ({ children }: PropsWithChildren<{}>) => {
  return (
    <GlobalContext.Provider
      value={{
        metricsContextLoaded: true,
        grafanaHost: undefined,
        dashboardUids: {
          default: "rayDefaultDashboard",
          serve: "rayServeDashboard",
          serveDeployment: "rayServeDeploymentDashboard",
          data: "rayDataDashboard",
        },
        prometheusHealth: false,
        sessionName: undefined,
        nodeMap: {},
        nodeMapByIp: {},
        namespaceMap: {},
        dashboardDatasource: "Prometheus",
      }}
    >
      {children}
    </GlobalContext.Provider>
  );
};

describe("ServeMetricsSection", () => {
  it("renders", async () => {
    expect.assertions(4);

    render(<ServeMetricsSection />, { wrapper: Wrapper });
    await screen.findByText(/View in Grafana/);
    expect(screen.getByText(/5 minutes/)).toBeVisible();
    expect(screen.getByTitle("QPS per application")).toBeInTheDocument();
    expect(screen.getByTitle("Error QPS per application")).toBeInTheDocument();
    expect(
      screen.getByTitle("P90 latency per application"),
    ).toBeInTheDocument();
  });

  it("renders nothing when grafana is not available", async () => {
    expect.assertions(5);

    render(<ServeMetricsSection />, { wrapper: MetricsDisabledWrapper });
    // Wait .1 seconds for render to finish
    await waitFor(() => new Promise((r) => setTimeout(r, 100)));

    expect(screen.queryByText(/View in Grafana/)).toBeNull();
    expect(screen.queryByText(/5 minutes/)).toBeNull();
    expect(screen.queryByTitle("QPS per application")).toBeNull();
    expect(screen.queryByTitle("Error QPS per application")).toBeNull();
    expect(screen.queryByTitle("P90 latency per application")).toBeNull();
  });
});
