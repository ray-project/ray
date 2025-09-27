import { render, screen } from "@testing-library/react";
import React, { PropsWithChildren } from "react";
import { GlobalContext } from "../../App";
import { STYLE_WRAPPER } from "../../util/test-utils";
import { Metrics } from "./Metrics";

const Wrapper = ({ children }: PropsWithChildren<{}>) => {
  return (
    <GlobalContext.Provider
      value={{
        metricsContextLoaded: true,
        grafanaHost: "localhost:3000",
        grafanaOrgId: "1",
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

describe("Metrics", () => {
  it("renders", async () => {
    expect.assertions(1);

    render(<Metrics />, { wrapper: Wrapper });
    await screen.findByText(/View in Grafana/);
    expect(
      screen.queryByText(
        /Set up Prometheus and Grafana for better Ray Dashboard experience/,
      ),
    ).toBeNull();
  });

  it("renders warning when ", async () => {
    expect.assertions(1);

    render(<Metrics />, { wrapper: MetricsDisabledWrapper });
    await screen.findByText(
      /Set up Prometheus and Grafana for better Ray Dashboard experience/,
    );
    expect(screen.queryByText(/View in Grafana/)).toBeNull();
  });
});
