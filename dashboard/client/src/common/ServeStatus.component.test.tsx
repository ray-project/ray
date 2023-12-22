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

    await screen.findByTestId("serve-status-icon");

    const icon = screen.getByTestId("serve-status-icon");
    const classList = icon.getAttribute("class");
    expect(classList).toContain("colorSuccess");
  });

  it("renders UNHEALTHY status", async () => {
    render(
      <ServeStatusIcon
        deployment={{ ...DEPLOYMENT, status: ServeDeploymentStatus.UNHEALTHY }}
        small={false}
      />,
    );

    await screen.findByTestId("serve-status-icon");

    expect(screen.queryByTestId("serve-status-icon")).not.toHaveClass(
      "colorSuccess",
    );
    expect(screen.queryByTestId("serve-status-icon")).not.toHaveClass(
      "colorError",
    );
  });

  it("renders UPDATING status", async () => {
    render(
      <ServeStatusIcon
        deployment={{ ...DEPLOYMENT, status: ServeDeploymentStatus.UPDATING }}
        small={false}
      />,
    );

    await screen.findByTestId("serve-status-icon");

    const icon = screen.getByTestId("serve-status-icon");
    const classList = icon.getAttribute("class");
    expect(classList).toContain("iconRunning");
  });
});
