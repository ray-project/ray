import { render, screen } from "@testing-library/react";
import React, { PropsWithChildren } from "react";
import { GlobalContext } from "../../App";
import { Metrics } from "./Metrics";

const Wrapper = ({ children }: PropsWithChildren<{}>) => {
  return (
    <GlobalContext.Provider
      value={{
        grafanaHost: "localhost:3000",
        grafanaDefaultDashboardUid: "rayDefaultDashboard",
        prometheusHealth: true,
        sessionName: "session-name",
        ipLogMap: {},
        nodeMap: {},
        nodeMapByIp: {},
        namespaceMap: {},
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
        grafanaHost: undefined,
        grafanaDefaultDashboardUid: undefined,
        prometheusHealth: false,
        sessionName: undefined,
        ipLogMap: {},
        nodeMap: {},
        nodeMapByIp: {},
        namespaceMap: {},
      }}
    >
      {children}
    </GlobalContext.Provider>
  );
};

describe("Metrics", () => {
  it("renders", async () => {
    expect.assertions(5);

    render(<Metrics newIA />, { wrapper: Wrapper });
    await screen.findByText(/View in Grafana/);
    expect(screen.getByText(/5 minutes/)).toBeVisible();
    expect(screen.getByText(/Tasks and Actors/)).toBeVisible();
    expect(screen.getByText(/Ray Resource Usage/)).toBeVisible();
    expect(screen.getByText(/Hardware Utilization/)).toBeVisible();
    expect(
      screen.queryByText(/Grafana or prometheus server not detected./),
    ).toBeNull();
  });

  it("renders warning when ", async () => {
    expect.assertions(5);

    render(<Metrics newIA />, { wrapper: MetricsDisabledWrapper });
    await screen.findByText(/Grafana or prometheus server not detected./);
    expect(screen.queryByText(/View in Grafana/)).toBeNull();
    expect(screen.queryByText(/5 minutes/)).toBeNull();
    expect(screen.queryByText(/Tasks and Actors/)).toBeNull();
    expect(screen.queryByText(/Ray Resource Usage/)).toBeNull();
    expect(screen.queryByText(/Hardware Utilization/)).toBeNull();
  });
});
