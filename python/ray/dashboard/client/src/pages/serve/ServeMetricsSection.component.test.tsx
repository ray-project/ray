import { render, screen, waitFor } from "@testing-library/react";
import React, { PropsWithChildren } from "react";
import { GlobalContext } from "../../App";
import { STYLE_WRAPPER } from "../../util/test-utils";
import {
  APPS_METRICS_CONFIG,
  SERVE_SYSTEM_METRICS_CONFIG,
  ServeMetricsSection,
} from "./ServeMetricsSection";

const Wrapper = ({ children }: PropsWithChildren<{}>) => {
  return (
    <GlobalContext.Provider
      value={{
        metricsContextLoaded: true,
        grafanaHost: "localhost:3000",
        grafanaOrgId: "1",
        grafanaClusterFilter: undefined,
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
        serverTimeZone: undefined,
        currentTimeZone: undefined,
      }}
    >
      <STYLE_WRAPPER>{children}</STYLE_WRAPPER>
    </GlobalContext.Provider>
  );
};

const MetricsDisabledWrapper = ({ children }: PropsWithChildren<{}>) => {
  return (
    <GlobalContext.Provider
      value={{
        metricsContextLoaded: true,
        grafanaHost: undefined,
        grafanaOrgId: "1",
        grafanaClusterFilter: undefined,
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
        serverTimeZone: undefined,
        currentTimeZone: undefined,
      }}
    >
      <STYLE_WRAPPER>{children}</STYLE_WRAPPER>
    </GlobalContext.Provider>
  );
};

describe("ServeMetricsSection", () => {
  it("renders app metrics", async () => {
    expect.assertions(4);

    render(<ServeMetricsSection metricsConfig={APPS_METRICS_CONFIG} />, {
      wrapper: Wrapper,
    });
    await screen.findByText(/View in Grafana/);
    expect(screen.getByText(/5 minutes/)).toBeVisible();
    expect(screen.getByTitle("QPS per application")).toBeInTheDocument();
    expect(screen.getByTitle("Error QPS per application")).toBeInTheDocument();
    expect(
      screen.getByTitle("P90 latency per application"),
    ).toBeInTheDocument();
  });

  it("renders serve system metrics", async () => {
    expect.assertions(6);

    render(
      <ServeMetricsSection metricsConfig={SERVE_SYSTEM_METRICS_CONFIG} />,
      {
        wrapper: Wrapper,
      },
    );
    await screen.findByText(/View in Grafana/);
    expect(screen.getByTitle("Ongoing HTTP Requests")).toBeInTheDocument();
    expect(screen.getByTitle("Ongoing gRPC Requests")).toBeInTheDocument();
    expect(screen.getByTitle("Scheduling Tasks")).toBeInTheDocument();
    expect(
      screen.getByTitle("Scheduling Tasks in Backoff"),
    ).toBeInTheDocument();
    expect(
      screen.getByTitle("Controller Control Loop Duration"),
    ).toBeInTheDocument();
    expect(screen.getByTitle("Number of Control Loops")).toBeInTheDocument();
  });

  it("renders nothing when grafana is not available", async () => {
    expect.assertions(5);

    render(<ServeMetricsSection metricsConfig={APPS_METRICS_CONFIG} />, {
      wrapper: MetricsDisabledWrapper,
    });
    // Wait .1 seconds for render to finish
    await waitFor(() => new Promise((r) => setTimeout(r, 100)));

    expect(screen.queryByText(/View in Grafana/)).toBeNull();
    expect(screen.queryByText(/5 minutes/)).toBeNull();
    expect(screen.queryByTitle("QPS per application")).toBeNull();
    expect(screen.queryByTitle("Error QPS per application")).toBeNull();
    expect(screen.queryByTitle("P90 latency per application")).toBeNull();
  });
});
