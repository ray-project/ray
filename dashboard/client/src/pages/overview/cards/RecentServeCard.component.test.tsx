import { render } from "@testing-library/react";
import React from "react";
import { getServeApplications } from "../../../service/serve";
import {
  ServeApplicationStatus,
  ServeDeploymentMode,
} from "../../../type/serve";
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
            deployments: {
              FirstDeployment: {},
              SecondDeployment: {},
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
              ThirdDeployment: {},
            },
          },
        },
      },
    } as any);
  });
  it("should display serve applications with and without deployed_app_config", () => {
    const { getByText } = render(<RecentServeCard />);

    expect(getByText("app1")).toBeInTheDocument();
    expect(getByText("/app1")).toBeInTheDocument();
    expect(getByText("dep1")).toBeInTheDocument();
    expect(getByText("app2")).toBeInTheDocument();
    expect(getByText("/app2")).toBeInTheDocument();
    expect(getByText("dep2")).toBeInTheDocument();
  });
});
