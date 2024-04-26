import { render, screen, within } from "@testing-library/react";
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
import { ServeDeploymentDetailPage } from "./ServeDeploymentDetailPage";

jest.mock("react-router-dom", () => ({
  ...jest.requireActual("react-router-dom"),
  useParams: jest.fn(),
}));
jest.mock("../../service/serve");

const mockUseParams = jest.mocked(useParams);
const mockGetServeApplications = jest.mocked(getServeApplications);

describe("ServeDeploymentDetailPage", () => {
  it("renders page with deployment details", async () => {
    expect.assertions(9);

    mockUseParams.mockReturnValue({
      applicationName: "home",
      deploymentName: "FirstDeployment",
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

    render(<ServeDeploymentDetailPage />, { wrapper: TEST_APP_WRAPPER });

    const user = userEvent.setup();

    await screen.findByText("FirstDeployment");
    expect(screen.getByTestId("metadata-content-for-Name")).toHaveTextContent(
      "FirstDeployment",
    );
    expect(screen.getByTestId("metadata-content-for-Status")).toHaveTextContent(
      "HEALTHY",
    );
    expect(
      screen.getByTestId("metadata-content-for-Replicas"),
    ).toHaveTextContent("2");

    // First replica
    // First instance of text is the Replica row
    // Second instance of text is the Replica selection in the dropdown
    expect(screen.getAllByText("test-replica-1")[0]).toBeVisible();
    expect(screen.getByText("STARTING")).toBeVisible();

    // Second replica
    expect(screen.getByText("test-replica-2")).toBeVisible();
    expect(screen.getByText("RUNNING")).toBeVisible();

    // Config dialog for deployment
    await user.click(
      within(
        screen.getByTestId("metadata-content-for-Deployment config"),
      ).getByText("View"),
    );
    await screen.findByText(/test-config: 1/);
    expect(screen.getByText(/test-config: 1/)).toBeVisible();
    expect(screen.getByText(/autoscaling-value: 2/)).toBeVisible();
  });
});
