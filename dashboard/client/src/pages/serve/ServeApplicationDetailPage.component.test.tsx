import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { useParams } from "react-router-dom";
import { getServeApplications } from "../../service/serve";
import {
  ServeApplicationStatus,
  ServeDeploymentStatus,
  ServeReplicaState,
} from "../../type/serve";
import { TEST_APP_WRAPPER } from "../../util/test-utils";
import { ServeApplicationDetailPage } from "./ServeApplicationDetailPage";

jest.mock("react-router-dom", () => ({
  ...jest.requireActual("react-router-dom"),
  useParams: jest.fn(),
}));
jest.mock("../../service/serve");

const mockUseParams = jest.mocked(useParams);
const mockGetServeApplications = jest.mocked(getServeApplications);

describe("ServeApplicationDetailPage", () => {
  it("renders page with deployments and replicas", async () => {
    expect.assertions(22);

    mockUseParams.mockReturnValue({
      applicationName: "home",
    });
    mockGetServeApplications.mockResolvedValue({
      data: {
        applications: {
          home: {
            name: "home",
            route_prefix: "/home",
            message: null,
            status: ServeApplicationStatus.RUNNING,
            deployed_app_config: {
              import_path: "home:graph",
            },
            last_deployed_time_s: new Date().getTime() / 1000,
            deployments: {
              FirstDeployment: {
                name: "FirstDeployment",
                deployment_config: {
                  "test-config": 1,
                  autoscaling_config: {
                    "autoscaling-value": 2,
                  },
                },
                status: ServeDeploymentStatus.HEALTHY,
                message: "deployment is healthy",
                replicas: [
                  {
                    actor_id: "test-actor-id",
                    actor_name: "FirstDeployment",
                    node_id: "test-node-id",
                    node_ip: "123.456.789.123",
                    pid: "12345",
                    replica_id: "test-replica-1",
                    start_time_s: new Date().getTime() / 1000,
                    state: ServeReplicaState.STARTING,
                  },
                  {
                    actor_id: "test-actor-id-2",
                    actor_name: "FirstDeployment",
                    node_id: "test-node-id",
                    node_ip: "123.456.789.123",
                    pid: "12346",
                    replica_id: "test-replica-2",
                    start_time_s: new Date().getTime() / 1000,
                    state: ServeReplicaState.RUNNING,
                  },
                ],
              },
              SecondDeployment: {
                name: "SecondDeployment",
                deployment_config: {},
                status: ServeDeploymentStatus.UPDATING,
                message: "deployment is updating",
                replicas: [
                  {
                    actor_id: "test-actor-id-3",
                    actor_name: "SecondDeployment",
                    node_id: "test-node-id",
                    node_ip: "123.456.789.123",
                    pid: "12347",
                    replica_id: "test-replica-3",
                    start_time_s: new Date().getTime() / 1000,
                    state: ServeReplicaState.STARTING,
                  },
                ],
              },
            },
          },
          "second-app": {
            // Decoy second app
          },
          "third-app": {
            // Decoy third app
          },
        },
      },
    } as any);

    render(<ServeApplicationDetailPage />, { wrapper: TEST_APP_WRAPPER });

    const user = userEvent.setup();

    await screen.findByText("home");
    expect(screen.getByTestId("metadata-content-for-Name")).toHaveTextContent(
      "home",
    );
    expect(
      screen.getByTestId("metadata-content-for-Route prefix"),
    ).toHaveTextContent("/home");
    expect(screen.getByTestId("metadata-content-for-Status")).toHaveTextContent(
      "RUNNING",
    );
    expect(
      screen.getByTestId("metadata-content-for-Replicas"),
    ).toHaveTextContent("3");

    // First deployment
    expect(screen.getByText("FirstDeployment")).toBeVisible();
    expect(screen.getByText("deployment is healthy")).toBeVisible();
    expect(screen.getByText("HEALTHY")).toBeVisible();

    // Second deployment
    expect(screen.getByText("SecondDeployment")).toBeVisible();
    expect(screen.getByText("deployment is updating")).toBeVisible();
    expect(screen.getByText("UPDATING")).toBeVisible();

    // Config dialog for application
    await user.click(
      within(
        screen.getByTestId("metadata-content-for-Application config"),
      ).getByText("View"),
    );
    await screen.findByText(/import_path: home:graph/);
    expect(screen.getByText(/import_path: home:graph/)).toBeVisible();

    // Config dialog for first deployment
    await user.click(screen.getAllByText("Deployment config")[0]);
    await screen.findByText(/test-config: 1/);
    expect(screen.getByText(/test-config: 1/)).toBeVisible();
    expect(screen.getByText(/autoscaling-value: 2/)).toBeVisible();

    // All deployments are already expanded
    expect(screen.getByText("test-replica-1")).toBeVisible();
    expect(screen.getByText("test-replica-2")).toBeVisible();
    expect(screen.getByText("test-replica-3")).toBeVisible();

    // Collapse the first deployment
    await user.click(screen.getAllByTitle("Collapse")[0]);
    await waitFor(() => screen.queryByText("test-replica-1") === null);
    expect(screen.queryByText("test-replica-1")).toBeNull();
    expect(screen.queryByText("test-replica-2")).toBeNull();
    expect(screen.getByText("test-replica-3")).toBeVisible();

    // Expand the first deployment again
    await user.click(screen.getByTitle("Expand"));
    await screen.findByText("test-replica-1");
    expect(screen.getByText("test-replica-1")).toBeVisible();
    expect(screen.getByText("test-replica-2")).toBeVisible();
    expect(screen.getByText("test-replica-3")).toBeVisible();
  });
});
