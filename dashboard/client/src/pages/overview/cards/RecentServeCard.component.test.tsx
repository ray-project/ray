import { render, screen } from "@testing-library/react";
import React from "react";
import { getServeApplications } from "../../../service/serve";
import {
  ServeApplicationStatus,
  ServeProxyLocation,
} from "../../../type/serve";
import { TEST_APP_WRAPPER } from "../../../util/test-utils";
import { RecentServeCard } from "./RecentServeCard";

jest.mock("../../../service/serve");

const mockGetServeApplications = jest.mocked(getServeApplications);

describe("RecentServeCard", () => {
  beforeEach(() => {
    mockGetServeApplications.mockResolvedValue({
      data: {
        http_options: { host: "1.2.3.4", port: 8000 },
        proxy_location: ServeProxyLocation.EveryNode,
        applications: {
          home: {
            name: "home",
            route_prefix: "/",
            message: null,
            status: ServeApplicationStatus.RUNNING,
            deployed_app_config: {
              import_path: "home:graph",
            },
            last_deployed_time_s: new Date().getTime() / 1000,
            deployments: {
              FirstDeployment: {
                name: "FirstDeployment",
              },
            },
          },
          "second-app": {
            name: "second-app",
            route_prefix: "/second-app",
            message: null,
            status: ServeApplicationStatus.DEPLOYING,
            deployed_app_config: null,
            last_deployed_time_s: new Date().getTime() / 1000,
            deployments: {
              SecondDeployment: {
                name: "SecondDeployment",
              },
            },
          },
        },
      },
    } as any);
  });

  it("should display serve deployments with deployed_app_config", async () => {
    render(<RecentServeCard />, {
      wrapper: TEST_APP_WRAPPER,
    });

    await screen.findByText("View all deployments");

    expect.assertions(3);
    expect(screen.getByText("FirstDeployment")).toBeInTheDocument();
    expect(screen.getByText("home:graph")).toBeInTheDocument();
    expect(screen.getByText("Serve Deployments")).toBeInTheDocument();
  });

  it("should display serve deployments without deployed_app_config", async () => {
    render(<RecentServeCard />, {
      wrapper: TEST_APP_WRAPPER,
    });

    await screen.findByText("View all deployments");

    expect.assertions(3);
    expect(screen.getByText("SecondDeployment")).toBeInTheDocument();
    expect(screen.getByText("second-app")).toBeInTheDocument(); // default value for no deployed_app_config
    expect(screen.getByText("Serve Deployments")).toBeInTheDocument();
  });

  it("should navigate to the applications page when the 'View all deployments' link is clicked", async () => {
    render(<RecentServeCard />, {
      wrapper: TEST_APP_WRAPPER,
    });

    await screen.findByText("View all deployments");
    const link = screen.getByRole("link", {
      name: /view all deployments/i,
    });
    expect(link).toHaveAttribute("href", "/serve");
  });
});
