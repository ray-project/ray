import { render, screen } from "@testing-library/react";
import React, { PropsWithChildren } from "react";
import { GlobalContext } from "../../App";
import { STYLE_WRAPPER } from "../../util/test-utils";
import { isGrafanaCloudHost, Metrics } from "./Metrics";

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
        themeMode: "light",
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        toggleTheme: () => {},
        showAcceleratorColumns: true,
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
        themeMode: "light",
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        toggleTheme: () => {},
        showAcceleratorColumns: true,
      }}
    >
      <STYLE_WRAPPER>{children}</STYLE_WRAPPER>
    </GlobalContext.Provider>
  );
};

describe("Metrics", () => {
  it("detects Grafana Cloud hosts", () => {
    expect(isGrafanaCloudHost("https://example.grafana.net")).toBe(true);
    expect(isGrafanaCloudHost("https://grafana.net")).toBe(true);
    expect(isGrafanaCloudHost("https://grafana.example.com")).toBe(false);
    expect(isGrafanaCloudHost(undefined)).toBe(false);
    expect(isGrafanaCloudHost("")).toBe(false);
    expect(isGrafanaCloudHost("not a valid url")).toBe(false);
  });

  it("renders", async () => {
    expect.assertions(4);

    render(<Metrics />, { wrapper: Wrapper });
    await screen.findByText(/View tab in Grafana/);
    expect(screen.getByText(/Core/)).toBeVisible();
    expect(screen.getByText(/Ray Data/)).toBeVisible();
    expect(document.querySelector("iframe")).toBeTruthy();
    expect(
      screen.queryByText(
        /Set up Prometheus and Grafana for better Ray Dashboard experience/,
      ),
    ).toBeNull();
  });

  it("renders warning when grafana is not available", async () => {
    expect.assertions(3);

    render(<Metrics />, { wrapper: MetricsDisabledWrapper });
    await screen.findByText(
      /Set up Prometheus and Grafana for better Ray Dashboard experience/,
    );
    expect(screen.queryByText(/View tab in Grafana/)).toBeNull();
    expect(screen.queryByText(/Core/)).toBeNull();
    expect(document.querySelector("iframe")).toBeNull();
  });

  it("validates iframe query parameters are correctly constructed", async () => {
    expect.assertions(11);

    render(<Metrics />, { wrapper: Wrapper });
    await screen.findByText(/View tab in Grafana/);

    // Get iframe element (should be only one)
    const iframes = document.querySelectorAll("iframe");
    expect(iframes.length).toBe(1);

    // Test the iframe to validate query parameters
    const iframe = iframes[0] as HTMLIFrameElement;
    const iframeSrc = iframe.src;
    const url = new URL(iframeSrc);

    // Validate required iframe query parameters
    expect(url.searchParams.get("orgId")).toBe("1");
    expect(url.searchParams.get("theme")).toBe("light");
    expect(url.searchParams.get("kiosk")).toBe("1");
    expect(url.searchParams.get("var-SessionName")).toBe("session-name");
    expect(url.searchParams.get("var-datasource")).toBe("Prometheus");
    expect(url.searchParams.get("refresh")).toBe("5s");
    expect(url.searchParams.get("from")).toBe("now-5m");
    expect(url.searchParams.get("to")).toBe("now");

    // Validate URL structure (full dashboard, not panel-only)
    expect(iframeSrc).toMatch(/localhost:3000\/d\/rayDefaultDashboard\/\?/);
    expect(iframeSrc).toContain("/d/rayDefaultDashboard");
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
            themeMode: "light",
            // eslint-disable-next-line @typescript-eslint/no-empty-function
            toggleTheme: () => {},
            showAcceleratorColumns: true,
          }}
        >
          <STYLE_WRAPPER>{children}</STYLE_WRAPPER>
        </GlobalContext.Provider>
      );
    };

    expect.assertions(2);

    render(<Metrics />, { wrapper: WrapperWithClusterFilter });
    await screen.findByText(/View tab in Grafana/);

    // Get the iframe and validate cluster filter parameter
    const iframes = document.querySelectorAll("iframe");
    const iframe = iframes[0] as HTMLIFrameElement;
    const iframeSrc = iframe.src;
    const url = new URL(iframeSrc);

    expect(url.searchParams.get("var-Cluster")).toBe("test-cluster");
    expect(iframeSrc).toContain("var-Cluster=test-cluster");
  });

  it("renders Grafana Cloud fallback instead of iframe", async () => {
    const GrafanaCloudWrapper = ({ children }: PropsWithChildren<{}>) => {
      return (
        <GlobalContext.Provider
          value={{
            metricsContextLoaded: true,
            grafanaHost: "https://ray-observability.grafana.net",
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
            themeMode: "light",
            // eslint-disable-next-line @typescript-eslint/no-empty-function
            toggleTheme: () => {},
            showAcceleratorColumns: true,
          }}
        >
          <STYLE_WRAPPER>{children}</STYLE_WRAPPER>
        </GlobalContext.Provider>
      );
    };

    expect.assertions(7);

    render(<Metrics />, { wrapper: GrafanaCloudWrapper });
    await screen.findByText(/Open this dashboard in Grafana Cloud/);

    expect(document.querySelector("iframe")).toBeNull();
    expect(screen.getByText(/Core/)).toBeVisible();

    const link = screen.getByRole("link", { name: /Open in Grafana/ });
    const url = new URL(link.getAttribute("href") ?? "");

    expect(url.origin).toBe("https://ray-observability.grafana.net");
    expect(url.pathname).toBe("/d/rayDefaultDashboard/");
    expect(url.searchParams.get("var-SessionName")).toBe("session-name");
    expect(url.searchParams.get("var-Cluster")).toBe("test-cluster");
    expect(url.searchParams.get("kiosk")).toBeNull();
  });
});
