import { render, screen } from "@testing-library/react";
import React from "react";
import { useParams } from "react-router-dom";
import { getServeApplications } from "../../service/serve";
import { ServeReplicaState } from "../../type/serve";
import { TEST_APP_WRAPPER } from "../../util/test-utils";
import { ServeReplicaDetailPage } from "./ServeReplicaDetailPage";

jest.mock("react-router-dom", () => ({
  ...jest.requireActual("react-router-dom"),
  useParams: jest.fn(),
}));
jest.mock("../../service/serve");

const mockUseParams = jest.mocked(useParams);
const mockGetServeApplications = jest.mocked(getServeApplications);

describe("ServeReplicaDetailPage", () => {
  it("renders", async () => {
    expect.assertions(9);

    mockUseParams.mockReturnValue({
      applicationName: "home",
      deploymentName: "FirstDeployment",
      replicaId: "test-replica-1",
    });
    mockGetServeApplications.mockResolvedValue({
      data: {
        applications: {
          home: {
            name: "home",
            route_prefix: "/home",
            deployments: {
              FirstDeployment: {
                name: "FirstDeployment",
                deployment_config: {
                  "test-config": 1,
                  autoscaling_config: {
                    "autoscaling-value": 2,
                  },
                },
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
                    // Decoy second replica
                  },
                ],
              },
              SecondDeployment: {
                name: "SecondDeployment",
                deployment_config: {},
                replicas: [
                  {
                    // Decoy third replica
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

    render(<ServeReplicaDetailPage />, { wrapper: TEST_APP_WRAPPER });

    await screen.findByTestId("metadata-content-for-ID");
    expect(screen.getByTestId("metadata-content-for-ID")).toHaveTextContent(
      "test-replica-1",
    );
    expect(screen.getByTestId("metadata-content-for-State")).toHaveTextContent(
      "STARTING",
    );
    expect(
      screen.getByTestId("metadata-content-for-Actor ID"),
    ).toHaveTextContent("test-actor-id");
    expect(
      screen.getByTestId("metadata-content-for-Actor name"),
    ).toHaveTextContent("FirstDeployment");
    expect(
      screen.getByTestId("metadata-content-for-Node ID"),
    ).toHaveTextContent("test-node-id");
    expect(
      screen.getByTestId("metadata-content-for-Node IP"),
    ).toHaveTextContent("123.456.789.123");
    expect(screen.getByTestId("metadata-content-for-PID")).toHaveTextContent(
      "12345",
    );
    expect(screen.getByText("Tasks History")).toBeVisible();
    expect(screen.getByText("Metrics")).toBeVisible();
  });
});
