import { render, screen, waitFor } from "@testing-library/react";
import React from "react";
import { ServeReplicaMetricsSection } from "./ServeDeploymentMetricsSection";
import { createServeMetricsTestWrapper } from "./serveMetricsTestUtils";

const Wrapper = createServeMetricsTestWrapper();
const WrapperWithClusterFilter = createServeMetricsTestWrapper({
  grafanaClusterFilter: "my-ray-cluster",
});
const MetricsDisabledWrapper = createServeMetricsTestWrapper({
  grafanaHost: undefined,
  prometheusHealth: false,
  sessionName: undefined,
});

describe("ServeReplicaMetricsSection", () => {
  it("renders", async () => {
    expect.assertions(4);

    render(
      <ServeReplicaMetricsSection
        deploymentName="test-deployment"
        replicaId="replica-1"
      />,
      { wrapper: Wrapper },
    );
    await screen.findByText(/View in Grafana/);
    expect(screen.getByText(/5 minutes/)).toBeVisible();
    expect(screen.getByTitle("QPS per replica")).toBeInTheDocument();
    expect(screen.getByTitle("Error QPS per replica")).toBeInTheDocument();
    expect(screen.getByTitle("P90 latency per replica")).toBeInTheDocument();
  });

  it("appends var-Cluster to Grafana URLs when grafanaClusterFilter is set", async () => {
    render(
      <ServeReplicaMetricsSection
        deploymentName="test-deployment"
        replicaId="replica-1"
      />,
      { wrapper: WrapperWithClusterFilter },
    );
    await screen.findByText(/View in Grafana/);

    const link = screen.getByRole("link", { name: /View in Grafana/i });
    expect(link.getAttribute("href")).toContain("var-Cluster=my-ray-cluster");

    const iframe = screen.getByTitle("QPS per replica");
    expect(iframe.getAttribute("src")).toContain("var-Cluster=my-ray-cluster");
  });

  it("renders nothing when grafana is not available", async () => {
    expect.assertions(5);

    render(
      <ServeReplicaMetricsSection
        deploymentName="test-deployment"
        replicaId="replica-1"
      />,
      { wrapper: MetricsDisabledWrapper },
    );
    // Wait .1 seconds for render to finish
    await waitFor(() => new Promise((r) => setTimeout(r, 100)));

    expect(screen.queryByText(/View in Grafana/)).toBeNull();
    expect(screen.queryByText(/5 minutes/)).toBeNull();
    expect(screen.queryByTitle("QPS per replica")).toBeNull();
    expect(screen.queryByTitle("Error QPS per replica")).toBeNull();
    expect(screen.queryByTitle("P90 latency per replica")).toBeNull();
  });
});
