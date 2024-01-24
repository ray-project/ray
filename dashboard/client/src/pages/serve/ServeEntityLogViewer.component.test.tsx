import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import {
  MultiTabLogViewer,
  MultiTabLogViewerTabDetails,
} from "../../common/MultiTabLogViewer";
import { TEST_APP_WRAPPER } from "../../util/test-utils";
import { ServeEntityLogViewer } from "./ServeEntityLogViewer";

jest.mock("../../common/MultiTabLogViewer");

const MockMultiTabLogViewer = jest.mocked(MultiTabLogViewer);

describe("ServeEntityLogViewer", () => {
  beforeEach(() => {
    MockMultiTabLogViewer.mockImplementation(
      ({ tabs }: { tabs: MultiTabLogViewerTabDetails[] }) => {
        return (
          <div>
            {tabs.map((tab) => (
              <div>
                {Object.entries(tab).map(([key, value]) => (
                  <span key={key}>
                    {key}: {value}
                  </span>
                ))}
              </div>
            ))}
          </div>
        );
      },
    );
  });

  it("renders with multiple entity groups", async () => {
    expect.assertions(43);

    render(
      <ServeEntityLogViewer
        controller={
          {
            node_id: "test-node-id-for-controller",
            log_file_path: "test-log-file-path-for-controller",
          } as any
        }
        proxies={
          [
            {
              actor_id: "test-actor-id-for-proxy-1",
              node_id: "test-node-id-for-proxy-1",
              log_file_path: "test-log-file-path-for-proxy-1",
            },
            {
              actor_id: "test-actor-id-for-proxy-2",
              node_id: "test-node-id-for-proxy-2",
              log_file_path: "test-log-file-path-for-proxy-2",
            },
          ] as any
        }
        deployments={
          [
            {
              name: "test-deployment-1",
              replicas: [
                {
                  replica_id: "test-replica-id-for-deployment-1",
                  actor_id: "test-actor-id-for-deployment-1",
                  node_id: "test-node-id-for-deployment-1",
                  log_file_path: "test-log-file-path-for-deployment-1",
                },
                {
                  replica_id: "test-replica-id-2-for-deployment-1",
                  actor_id: "test-actor-id-2-for-deployment-1",
                  node_id: "test-node-id-2-for-deployment-1",
                  log_file_path: "test-log-file-path-2-for-deployment-1",
                },
              ],
            },
            {
              name: "test-deployment-2",
              replicas: [
                {
                  replica_id: "test-replica-id-for-deployment-2",
                  actor_id: "test-actor-id-for-deployment-2",
                  node_id: "test-node-id-for-deployment-2",
                  log_file_path: "test-log-file-path-for-deployment-2",
                },
              ],
            },
          ] as any
        }
      />,
      { wrapper: TEST_APP_WRAPPER },
    );

    await screen.findByText("View logs from");

    const user = userEvent.setup();

    // Verify dropdowns are rendered
    expect(screen.getByText("View logs from")).toBeVisible();
    expect(screen.getByRole("button", { name: "Controller" })).toBeVisible();
    expect(screen.queryByText("HTTP Proxy")).not.toBeInTheDocument();
    expect(screen.queryByText("Deployment replicas")).not.toBeInTheDocument();

    // Verify Controller logs are rendered
    expect(screen.getByText("title: Controller logs")).toBeVisible();
    expect(
      screen.getByText("nodeId: test-node-id-for-controller"),
    ).toBeVisible();
    expect(
      screen.getByText("filename: test-log-file-path-for-controller"),
    ).toBeVisible();
    expect(
      screen.queryByText("title: HTTP Proxy logs"),
    ).not.toBeInTheDocument();

    // Verify HTTP Proxy logs are rendered
    await user.click(screen.getByRole("button", { name: "Controller" }));
    await screen.findByText(/Proxies/);
    await user.click(screen.getByRole("option", { name: /Proxies/ }));
    await screen.findByText("HTTP Proxy");

    expect(
      screen.getByRole("button", {
        name: "HTTPProxyActor:test-actor-id-for-proxy-1",
      }),
    ).toBeVisible();

    expect(screen.getByText("title: HTTP Proxy logs")).toBeVisible();
    expect(screen.getByText("nodeId: test-node-id-for-proxy-1")).toBeVisible();
    expect(
      screen.getByText("filename: test-log-file-path-for-proxy-1"),
    ).toBeVisible();

    // Switch to proxy 2
    await user.click(
      screen.getByRole("button", {
        name: "HTTPProxyActor:test-actor-id-for-proxy-1",
      }),
    );
    await screen.findByText("HTTPProxyActor:test-actor-id-for-proxy-2");
    await user.click(
      screen.getByRole("option", {
        name: "HTTPProxyActor:test-actor-id-for-proxy-2",
      }),
    );
    await screen.findByText("nodeId: test-node-id-for-proxy-2");

    expect(screen.getByText("title: HTTP Proxy logs")).toBeVisible();
    expect(screen.getByText("nodeId: test-node-id-for-proxy-2")).toBeVisible();
    expect(
      screen.getByText("filename: test-log-file-path-for-proxy-2"),
    ).toBeVisible();

    // Verify Deployment logs are rendered
    await user.click(screen.getByRole("button", { name: "Proxies" }));
    await screen.findByText(/Deployments/);
    await user.click(screen.getByRole("option", { name: /Deployments/ }));
    await screen.findByText("Deployment replica");

    expect(
      screen.getByRole("button", {
        name: "test-replica-id-for-deployment-1",
      }),
    ).toBeVisible();

    expect(screen.getByText("title: Serve logger")).toBeVisible();
    expect(
      screen.getByText("nodeId: test-node-id-for-deployment-1"),
    ).toBeVisible();
    expect(
      screen.getByText("filename: test-log-file-path-for-deployment-1"),
    ).toBeVisible();
    expect(screen.getByText("title: stderr")).toBeVisible();
    expect(
      screen.getAllByText("actorId: test-actor-id-for-deployment-1")[0],
    ).toBeVisible();
    expect(screen.getByText("suffix: err")).toBeVisible();
    expect(screen.getByText("title: stderr")).toBeVisible();
    expect(
      screen.getAllByText("actorId: test-actor-id-for-deployment-1")[1],
    ).toBeVisible();
    expect(screen.getByText("suffix: out")).toBeVisible();

    // Switch to replica 2
    await user.click(
      screen.getByRole("button", {
        name: "test-replica-id-for-deployment-1",
      }),
    );
    await screen.findByText("test-replica-id-2-for-deployment-1");
    await user.click(
      screen.getByRole("option", {
        name: "test-replica-id-2-for-deployment-1",
      }),
    );
    await screen.findByText("nodeId: test-node-id-2-for-deployment-1");

    expect(screen.getByText("title: Serve logger")).toBeVisible();
    expect(
      screen.getByText("nodeId: test-node-id-2-for-deployment-1"),
    ).toBeVisible();
    expect(
      screen.getByText("filename: test-log-file-path-2-for-deployment-1"),
    ).toBeVisible();
    expect(screen.getByText("title: stderr")).toBeVisible();
    expect(
      screen.getAllByText("actorId: test-actor-id-2-for-deployment-1")[0],
    ).toBeVisible();
    expect(screen.getByText("suffix: err")).toBeVisible();
    expect(screen.getByText("title: stderr")).toBeVisible();
    expect(
      screen.getAllByText("actorId: test-actor-id-2-for-deployment-1")[1],
    ).toBeVisible();
    expect(screen.getByText("suffix: out")).toBeVisible();

    // Switch to replica 3
    await user.click(
      screen.getByRole("button", {
        name: "test-replica-id-2-for-deployment-1",
      }),
    );
    await screen.findByText("test-replica-id-for-deployment-2");
    await user.click(
      screen.getByRole("option", {
        name: "test-replica-id-for-deployment-2",
      }),
    );
    await screen.findByText("nodeId: test-node-id-for-deployment-2");

    expect(screen.getByText("title: Serve logger")).toBeVisible();
    expect(
      screen.getByText("nodeId: test-node-id-for-deployment-2"),
    ).toBeVisible();
    expect(
      screen.getByText("filename: test-log-file-path-for-deployment-2"),
    ).toBeVisible();
    expect(screen.getByText("title: stderr")).toBeVisible();
    expect(
      screen.getAllByText("actorId: test-actor-id-for-deployment-2")[0],
    ).toBeVisible();
    expect(screen.getByText("suffix: err")).toBeVisible();
    expect(screen.getByText("title: stderr")).toBeVisible();
    expect(
      screen.getAllByText("actorId: test-actor-id-for-deployment-2")[1],
    ).toBeVisible();
    expect(screen.getByText("suffix: out")).toBeVisible();
  });

  it("renders only deployments when controller and proxies are not passed in", async () => {
    expect.assertions(5);

    render(
      <ServeEntityLogViewer
        deployments={
          [
            {
              name: "test-deployment-1",
              replicas: [
                {
                  replica_id: "test-replica-id-for-deployment-1",
                  actor_id: "test-actor-id-for-deployment-1",
                  node_id: "test-node-id-for-deployment-1",
                  log_file_path: "test-log-file-path-for-deployment-1",
                },
                {
                  replica_id: "test-replica-id-2-for-deployment-1",
                  actor_id: "test-actor-id-2-for-deployment-1",
                  node_id: "test-node-id-2-for-deployment-1",
                  log_file_path: "test-log-file-path-2-for-deployment-1",
                },
              ],
            },
            {
              name: "test-deployment-2",
              replicas: [
                {
                  replica_id: "test-replica-id-for-deployment-2",
                  actor_id: "test-actor-id-for-deployment-2",
                  node_id: "test-node-id-for-deployment-2",
                  log_file_path: "test-log-file-path-for-deployment-2",
                },
              ],
            },
          ] as any
        }
      />,
      { wrapper: TEST_APP_WRAPPER },
    );

    await screen.findByText("Deployment replica");

    // Verify dropdowns are rendered
    expect(screen.queryByText("View logs from")).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "Controller" }),
    ).not.toBeInTheDocument();
    expect(screen.queryByText("HTTP Proxy")).not.toBeInTheDocument();
    expect(screen.getByText("Deployment replica")).toBeVisible();
    expect(
      screen.getByRole("button", { name: "test-replica-id-for-deployment-1" }),
    ).toBeVisible();
  });
});
