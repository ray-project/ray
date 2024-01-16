import { render, screen } from "@testing-library/react";
import React from "react";
import { ServeDeployment, ServeDeploymentStatus } from "../type/serve";
import { ServeStatusIcon } from "./ServeStatus";

const DEPLOYMENT: ServeDeployment = {
  name: "MyServeDeployment",
  deployment_config: {} as any,
  message: "Running",
  replicas: [],
  status: ServeDeploymentStatus.HEALTHY,
};

describe("ServeStatusIcon", () => {
  it("renders HEALTHY status", async () => {
    render(<ServeStatusIcon deployment={DEPLOYMENT} small={false} />);

    await screen.findByTitle("Healthy");
  });

  it("renders UNHEALTHY status", async () => {
    render(
      <ServeStatusIcon
        deployment={{ ...DEPLOYMENT, status: ServeDeploymentStatus.UNHEALTHY }}
        small={false}
      />,
    );

    await screen.findByTitle("Unhealthy");
  });

  it("renders UPDATING status", async () => {
    render(
      <ServeStatusIcon
        deployment={{ ...DEPLOYMENT, status: ServeDeploymentStatus.UPDATING }}
        small={false}
      />,
    );

    await screen.findByTitle("Updating");
  });
});
