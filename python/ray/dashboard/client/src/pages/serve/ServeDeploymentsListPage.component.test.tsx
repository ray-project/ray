import { render, screen } from "@testing-library/react";
import React from "react";
import { getActor } from "../../service/actor";
import { getServeApplications } from "../../service/serve";
import {
  ServeApplicationStatus,
  ServeProxyLocation,
  ServeSystemActorStatus,
} from "../../type/serve";
import { TEST_APP_WRAPPER } from "../../util/test-utils";
import { ServeDeploymentsListPage } from "./ServeDeploymentsListPage";

jest.mock("../../service/actor");
jest.mock("../../service/serve");

const mockGetServeApplications = jest.mocked(getServeApplications);
const mockGetActor = jest.mocked(getActor);

describe("ServeDeploymentsListPage", () => {
  it("renders list", async () => {
    expect.assertions(4);

    // Mock ServeController actor fetch
    mockGetActor.mockResolvedValue({
      data: {
        data: {
          detail: {
            state: "ALIVE",
          },
        },
      },
    } as any);

    mockGetServeApplications.mockResolvedValue({
      data: {
        http_options: { host: "1.2.3.4", port: 8000 },
        proxies: {
          foo: {
            node_id: "node:12345",
            status: ServeSystemActorStatus.STARTING,
            actor_id: "actor:12345",
          },
        },
        controller_info: {
          node_id: "node:12345",
          actor_id: "actor:12345",
        },
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
                replicas: [],
              },
              SecondDeployment: {
                name: "SecondDeployment",
                replicas: [],
              },
            },
          },
          "second-app": {
            name: "second-app",
            route_prefix: "/second-app",
            message: null,
            status: ServeApplicationStatus.DEPLOYING,
            deployed_app_config: {
              import_path: "second_app:graph",
            },
            last_deployed_time_s: new Date().getTime() / 1000,
            deployments: {
              ThirdDeployment: {
                name: "ThirdDeployment",
                replicas: [],
              },
            },
          },
        },
      },
    } as any);

    render(<ServeDeploymentsListPage />, { wrapper: TEST_APP_WRAPPER });
    await screen.findByText("Application status");

    // First row
    expect(screen.getByText("FirstDeployment")).toBeVisible();

    // Second row
    expect(screen.getByText("SecondDeployment")).toBeVisible();

    // Third row
    expect(screen.getByText("ThirdDeployment")).toBeVisible();
    expect(screen.getByText("/second-app")).toBeVisible();
  });
});
