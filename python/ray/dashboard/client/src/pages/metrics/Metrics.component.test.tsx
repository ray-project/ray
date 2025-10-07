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

  it("renders warning when grafana is not available", async () => {
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

  it("validates iframe query parameters are correctly constructed", async () => {
    expect.assertions(11);

    render(<Metrics />, { wrapper: Wrapper });
    await screen.findByText(/View in Grafana/);

    // Get all iframe elements
    const iframes = document.querySelectorAll("iframe");
    expect(iframes.length).toBeGreaterThan(0);

    // Test the first iframe to validate query parameters
    const firstIframe = iframes[0] as HTMLIFrameElement;
    const iframeSrc = firstIframe.src;
    const url = new URL(iframeSrc);

    // Validate required iframe query parameters
    expect(url.searchParams.get("orgId")).toBe("1");
    expect(url.searchParams.get("theme")).toBe("light");
    expect(url.searchParams.get("panelId")).toBeTruthy();
    expect(url.searchParams.get("var-SessionName")).toBe("session-name");
    expect(url.searchParams.get("var-datasource")).toBe("Prometheus");
    expect(url.searchParams.get("refresh")).toBe("5s");
    expect(url.searchParams.get("from")).toBe("now-5m");
    expect(url.searchParams.get("to")).toBe("now");

    // Validate URL structure
    expect(iframeSrc).toMatch(/localhost:3000\/d-solo\/rayDefaultDashboard\?/);
    expect(iframeSrc).toContain("/d-solo/rayDefaultDashboard");
  });

  it("validates iframe query parameters with cluster filter", async () => {
    const WrapperWithClusterFilter = ({ children }: PropsWithChildren<{}>) => {
      return (
        <GlobalContext.Provider
          value={{
            metricsContextLoaded: true,
            grafanaHost: "localhost:3000",
            grafanaOrgId: "1",
            grafanaClusterFilter: "test-cluster",
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

    expect.assertions(2);

    render(<Metrics />, { wrapper: WrapperWithClusterFilter });
    await screen.findByText(/View in Grafana/);

    // Get the first iframe and validate cluster filter parameter
    const iframes = document.querySelectorAll("iframe");
    const firstIframe = iframes[0] as HTMLIFrameElement;
    const iframeSrc = firstIframe.src;
    const url = new URL(iframeSrc);

    expect(url.searchParams.get("var-Cluster")).toBe("test-cluster");
    expect(iframeSrc).toContain("var-Cluster=test-cluster");
  });
});
