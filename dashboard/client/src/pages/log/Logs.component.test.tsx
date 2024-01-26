import { render, screen } from "@testing-library/react";
import React from "react";
import { MemoryRouter } from "react-router-dom";
import { SWRConfig } from "swr";
import { listStateApiLogs } from "../../service/log";
import { getNodeList } from "../../service/node";
import { useStateApiLogs } from "./hooks";
import { StateApiLogsListPage, StateApiLogViewerPage } from "./Logs";

jest.mock("../../service/node");
jest.mock("../../service/log");
jest.mock("./hooks");

const mockGetNodeList = jest.mocked(getNodeList);
const mockListStateApiLogs = jest.mocked(listStateApiLogs);
const mockUseStateApiLogs = jest.mocked(useStateApiLogs);

describe("LogsPage", () => {
  it("renders a nodes view", async () => {
    expect.assertions(5);

    mockGetNodeList.mockResolvedValueOnce({
      data: {
        data: {
          summary: [
            {
              ip: "127.0.0.1",
              raylet: {
                nodeId: "node-id-1",
                state: "ALIVE",
              },
            },
            {
              ip: "127.0.0.2",
              raylet: {
                nodeId: "node-id-2",
                state: "ALIVE",
              },
            },
            {
              ip: "127.0.0.3",
              raylet: {
                nodeId: "node-id-3",
                state: "DEAD",
              },
            },
          ],
        },
      },
    } as any);

    render(
      <MemoryRouter
        initialEntries={[
          {
            // No params should show nodes list
            search: "",
          },
        ]}
      >
        <StateApiLogsListPage />
      </MemoryRouter>,
    );
    await screen.findByText("Select a node to view logs");

    expect(
      screen.getByText("Node ID: node-id-1 (IP: 127.0.0.1)"),
    ).toBeVisible();
    expect(
      screen.getByRole("link", { name: /Node ID: node-id-1/ }),
    ).toHaveAttribute("href", "/?nodeId=node-id-1");
    expect(
      screen.getByText("Node ID: node-id-2 (IP: 127.0.0.2)"),
    ).toBeVisible();
    expect(
      screen.getByRole("link", { name: /Node ID: node-id-2/ }),
    ).toHaveAttribute("href", "/?nodeId=node-id-2");
    expect(
      screen.queryByText("Node ID: node-id-3 (IP: 127.0.0.3)"),
    ).not.toBeInTheDocument();
  });

  it("renders a list view of files", async () => {
    expect.assertions(6);

    mockListStateApiLogs.mockResolvedValueOnce({
      data: {
        data: {
          result: {
            dashboard: ["dashboard.log", "dashboard.err", "dashboard.out"],
            internal: ["events/", "monitor.out"],
          },
        },
      },
    } as any);

    render(
      <MemoryRouter
        initialEntries={[
          {
            // Only a nodeId param should show files list for the root of that node.
            search: "nodeId=node-id-1",
          },
        ]}
      >
        <StateApiLogsListPage />
      </MemoryRouter>,
    );
    await screen.findByText(`Node: node-id-1`);

    expect(screen.getByRole("button", { name: "Back To ../" })).toHaveAttribute(
      "href",
      "/logs/",
    );

    expect(screen.getByRole("link", { name: "dashboard.err" })).toHaveAttribute(
      "href",
      "/viewer?nodeId=node-id-1&fileName=dashboard.err",
    );
    expect(screen.getByRole("link", { name: "dashboard.out" })).toHaveAttribute(
      "href",
      "/viewer?nodeId=node-id-1&fileName=dashboard.out",
    );
    expect(screen.getByRole("link", { name: "dashboard.log" })).toHaveAttribute(
      "href",
      "/viewer?nodeId=node-id-1&fileName=dashboard.log",
    );
    expect(screen.getByRole("link", { name: "monitor.out" })).toHaveAttribute(
      "href",
      "/viewer?nodeId=node-id-1&fileName=monitor.out",
    );
    expect(screen.getByRole("link", { name: "events/" })).toHaveAttribute(
      "href",
      "/?nodeId=node-id-1&folder=events",
    );
  });

  it("renders a list view of files in a folder", async () => {
    expect.assertions(4);

    mockListStateApiLogs.mockResolvedValueOnce({
      data: {
        data: {
          result: {
            internal: [
              "events/events_RAYLET.log",
              "events/events_AUTOSCALER.log",
              "events/secondFolder/",
            ],
          },
        },
      },
    } as any);

    render(
      <MemoryRouter
        initialEntries={[
          {
            search: "nodeId=node-id-1&folder=events",
          },
        ]}
      >
        <SWRConfig value={{ provider: () => new Map() }}>
          <StateApiLogsListPage />
        </SWRConfig>
      </MemoryRouter>,
    );
    await screen.findByText(`Node: node-id-1`);

    expect(screen.getByRole("button", { name: "Back To ../" })).toHaveAttribute(
      "href",
      "/logs/?nodeId=node-id-1&folder=",
    );

    expect(
      screen.getByRole("link", { name: "events_RAYLET.log" }),
    ).toHaveAttribute(
      "href",
      "/viewer?nodeId=node-id-1&fileName=events%2Fevents_RAYLET.log",
    );
    expect(
      screen.getByRole("link", { name: "events_AUTOSCALER.log" }),
    ).toHaveAttribute(
      "href",
      "/viewer?nodeId=node-id-1&fileName=events%2Fevents_AUTOSCALER.log",
    );
    expect(screen.getByRole("link", { name: "secondFolder/" })).toHaveAttribute(
      "href",
      "/?nodeId=node-id-1&folder=events%2FsecondFolder",
    );
  });

  it("renders a log viewer", async () => {
    expect.assertions(4);

    mockUseStateApiLogs.mockReturnValue({
      downloadUrl: "www.download.com",
      log: "SOME LOGS",
      path: "some/path",
      refresh: (() => {
        /* empty */
      }) as any,
    });

    render(
      <MemoryRouter
        initialEntries={[
          {
            search: "nodeId=node-id-1&fileName=dashboard.log",
          },
        ]}
      >
        <StateApiLogViewerPage />
      </MemoryRouter>,
    );
    await screen.findByText(`Node: node-id-1`);
    expect(screen.getByText("File: dashboard.log")).toBeVisible();

    expect(screen.getByRole("button", { name: "Back To ../" })).toHaveAttribute(
      "href",
      "/logs/?nodeId=node-id-1&folder=",
    );

    await screen.findByText("SOME");
    expect(screen.getByText("SOME")).toBeVisible();
    expect(screen.getByText("LOGS")).toBeVisible();
  });
});
