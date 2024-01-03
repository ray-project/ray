import { render, screen, waitFor } from "@testing-library/react";
import React, { PropsWithChildren } from "react";
import { MemoryRouter } from "react-router-dom";
import { GlobalContext } from "../../App";
import { useJobList } from "../job/hook/useJobList";
import { OverviewPage } from "./OverviewPage";

jest.mock("../job/hook/useJobList");

describe("OverviewPage", () => {
  it("renders", async () => {
    expect.assertions(3);

    const mockedUseJobList = jest.mocked(useJobList);

    mockedUseJobList.mockReturnValue({
      jobList: [
        {
          job_id: "01000000",
          submission_id: "raysubmit_12345",
          status: "SUCCEEDED",
        },
      ],
    } as any);

    render(<OverviewPage />, { wrapper: Wrapper(false) });
    await screen.findByText(/Events/);
    expect(screen.getByText(/Events/)).toBeVisible();
    expect(screen.getByTitle("Cluster Utilization")).toBeInTheDocument();
    expect(screen.getByTitle("Node Count")).toBeInTheDocument();
    // Wait because for some reason events is trying to log something after test is finished
    await waitFor(() => new Promise((resolve) => setTimeout(resolve, 10)));
  });

  it("does not render metrics cards if grafana is disabled", async () => {
    expect.assertions(5);

    const mockedUseJobList = jest.mocked(useJobList);

    mockedUseJobList.mockReturnValue({
      jobList: [
        {
          job_id: "01000000",
          submission_id: "raysubmit_12345",
          status: "SUCCEEDED",
        },
      ],
    } as any);

    render(<OverviewPage />, { wrapper: Wrapper(true) });
    await screen.findByText(/Events/);
    expect(screen.getByText(/Events/)).toBeVisible();
    expect(screen.queryByText("Cluster utilization")).toBeNull();
    expect(screen.queryByTitle("Cluster Utilization")).toBeNull();
    expect(screen.queryByText("Node count")).toBeNull();
    expect(screen.queryByTitle("Node Count")).toBeNull();
    // Wait because for some reason events is trying to log something after test is finished
    await waitFor(() => new Promise((resolve) => setTimeout(resolve, 10)));
  });
});

const Wrapper =
  (grafanaHostDisabled: boolean) =>
  ({ children }: PropsWithChildren<{}>) => {
    return (
      <MemoryRouter>
        <GlobalContext.Provider
          value={{
            metricsContextLoaded: true,
            grafanaHost: grafanaHostDisabled
              ? "DISABLED"
              : "http://localhost:3000",
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
      </MemoryRouter>
    );
  };
