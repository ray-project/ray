import { render, screen } from "@testing-library/react";
import React from "react";
import { getServeApplications } from "../../../service/serve";
import {
  ServeApplicationStatus,
  ServeDeploymentMode,
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
        proxy_location: ServeDeploymentMode.EveryNode,
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
          },
          "second-app": {
            name: "second-app",
            route_prefix: "/second-app",
            message: null,
            status: ServeApplicationStatus.DEPLOYING,
            deployed_app_config: null,
            last_deployed_time_s: new Date().getTime() / 1000,
            deployments: {},
          },
        },
      },
    } as any);
  });

  it("should display serve applications with deployed_app_config", async () => {
    render(<RecentServeCard />, {
      wrapper: TEST_APP_WRAPPER,
    });

    await screen.findByText("View all applications");

    expect.assertions(3);
    expect(screen.getByText("home")).toBeInTheDocument();
    expect(screen.getByText("home:graph")).toBeInTheDocument();
    expect(screen.getByText("Serve Applications")).toBeInTheDocument();
  });

  it("should display serve applications without deployed_app_config", async () => {
    render(<RecentServeCard />, {
      wrapper: TEST_APP_WRAPPER,
    });

    await screen.findByText("View all applications");

    expect.assertions(3);
    expect(screen.getByText("second-app")).toBeInTheDocument();
    expect(screen.getByText("-")).toBeInTheDocument(); // default value for no deployed_app_config
    expect(screen.getByText("Serve Applications")).toBeInTheDocument();
  });

  it("should navigate to the applications page when the 'View all applications' link is clicked", async () => {
    render(<RecentServeCard />, {
      wrapper: TEST_APP_WRAPPER,
    });

    await screen.findByText("View all applications");
    const link = screen.getByRole("link", {
      name: /view all applications/i,
    });
    expect(link).toHaveAttribute("href");
  });
});
