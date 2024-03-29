import { render, screen } from "@testing-library/react";
import React, { PropsWithChildren } from "react";
import { GlobalContext } from "../../App";
import { Metrics } from "./Metrics";

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

describe("Metrics", () => {
  it("renders", async () => {
    expect.assertions(5);

    render(<Metrics />, { wrapper: Wrapper });
    await screen.findByText(/View in Grafana/);
    expect(screen.getByText(/5 minutes/)).toBeVisible();
    expect(screen.getByText(/Tasks and Actors/)).toBeVisible();
    expect(screen.getByText(/Ray Resource Usage/)).toBeVisible();
    expect(screen.getByText(/Hardware Utilization/)).toBeVisible();
    expect(
      screen.queryByText(
        /Set up Prometheus and Grafana for better Ray Dashboard experience/,
      ),
    ).toBeNull();
  });

  it("renders warning when ", async () => {
    expect.assertions(5);

    render(<Metrics />, { wrapper: MetricsDisabledWrapper });
    await screen.findByText(
      /Set up Prometheus and Grafana for better Ray Dashboard experience/,
    );
    expect(screen.queryByText(/View in Grafana/)).toBeNull();
    expect(screen.queryByText(/5 minutes/)).toBeNull();
    expect(screen.queryByText(/Tasks and Actors/)).toBeNull();
    expect(screen.queryByText(/Ray Resource Usage/)).toBeNull();
    expect(screen.queryByText(/Hardware Utilization/)).toBeNull();
  });
});
