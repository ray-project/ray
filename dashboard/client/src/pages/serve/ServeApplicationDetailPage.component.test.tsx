import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { useParams } from "react-router-dom";
import { getServeApplications } from "../../service/serve";
import {
  ServeApplicationStatus,
  ServeDeploymentStatus,
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
  it("renders page with deployments", async () => {
    expect.assertions(12);

    mockUseParams.mockReturnValue({
      name: "home",
    });
    mockGetServeApplications.mockResolvedValue({
      data: {
        application_details: {
          home: {
            name: "home",
            route_prefix: "/home",
            app_message: null,
            app_status: ServeApplicationStatus.RUNNING,
            deployed_app_config: {
              import_path: "home:graph",
            },
            deployment_timestamp: new Date().getTime() / 1000,
            deployments_details: {
              FirstDeployment: {
                name: "FirstDeployment",
                deployment_config: {
                  "test-config": 1,
                  autoscaling_config: {
                    "autoscaling-value": 2,
                  },
                },
                deployment_status: ServeDeploymentStatus.HEALTHY,
                message: "deployment is healthy",
              },
              SecondDeployment: {
                name: "SecondDeployment",
                deployment_config: {},
                deployment_status: ServeDeploymentStatus.UPDATING,
                message: "deployment is updating",
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
    expect(screen.getByText("home")).toBeVisible();
    expect(screen.getByText("/home")).toBeVisible();
    expect(screen.getByText("RUNNING")).toBeVisible();

    // First deployment
    expect(screen.getByText("FirstDeployment")).toBeVisible();
    expect(screen.getByText("deployment is healthy")).toBeVisible();
    expect(screen.getByText("HEALTHY")).toBeVisible();

    // Second deployment
    expect(screen.getByText("SecondDeployment")).toBeVisible();
    expect(screen.getByText("deployment is updating")).toBeVisible();
    expect(screen.getByText("UPDATING")).toBeVisible();

    // Config dialog for application
    user.click(screen.getAllByText("View")[0]);
    await screen.findByText(/"import_path": "home:graph"/);
    expect(screen.getByText(/"import_path": "home:graph"/)).toBeVisible();

    // Config dialog for first deployment
    user.click(screen.getAllByText("View")[1]);
    await screen.findByText(/"test-config": 1/);
    expect(screen.getByText(/"test-config": 1/)).toBeVisible();
    expect(screen.getByText(/"autoscaling-value": 2/)).toBeVisible();
  });
});
