import { render, screen } from "@testing-library/react";
import React, { PropsWithChildren } from "react";
import { GlobalContext } from "../../App";
import { Metrics } from "./Metrics";

const Wrapper = ({ children }: PropsWithChildren<{}>) => {
  return (
    <GlobalContext.Provider
      value={{
        grafanaHost: "localhost:3000",
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
    expect.assertions(6);

    render(<Metrics newIA />, { wrapper: Wrapper });
    await screen.findByText(/View in Grafana/);
    expect(screen.getByText(/5 minutes/)).toBeVisible();
    expect(screen.getByText(/Tasks/)).toBeVisible();
    expect(screen.getByText(/Actors/)).toBeVisible();
    expect(screen.getByText(/Scheduler and autoscaler/)).toBeVisible();
    expect(screen.getByText(/Node metrics/)).toBeVisible();
    expect(
      screen.queryByText(/Grafana or prometheus server not detected./),
    ).toBeNull();
  });

  it("renders warning when ", async () => {
    expect.assertions(6);

    render(<Metrics newIA />, { wrapper: MetricsDisabledWrapper });
    await screen.findByText(/Grafana or prometheus server not detected./);
    expect(screen.queryByText(/View in Grafana/)).toBeNull();
    expect(screen.queryByText(/5 minutes/)).toBeNull();
    expect(screen.queryByText(/Tasks/)).toBeNull();
    expect(screen.queryByText(/Actors/)).toBeNull();
    expect(screen.queryByText(/Scheduler and autoscaler/)).toBeNull();
    expect(screen.queryByText(/Node metrics/)).toBeNull();
  });
});
