import { render, screen } from "@testing-library/react";
import { noop } from "lodash";
import React from "react";
import { MemoryRouter } from "react-router-dom";
import { NodeDetail } from "../../type/node";
import { CoreWorkerStats, Worker } from "../../type/worker";
import { NodeRow, WorkerRow } from "./NodeRow";

const NODE: NodeDetail = {
  hostname: "test-hostname",
  ip: "192.168.0.1",
  cpu: 15,
  mem: [100, 95, 5],
  state: "ALIVE",
  disk: {
    "/": {
      used: 20000000,
      total: 200000000,
      free: 180000000,
      percent: 10,
    },
    "/tmp": {
      used: 0,
      total: 200,
      free: 200,
      percent: 0,
    },
  },
  networkSpeed: [5, 10],
  raylet: {
    state: "ALIVE",
    nodeId: "1234567890ab",
    isHeadNode: true,
    numWorkers: 0,
    pid: 2345,
    startTime: 100,
    terminateTime: -1,
    brpcPort: 3456,
    nodeManagerPort: 5890,
    objectStoreAvailableMemory: 40,
    objectStoreUsedMemory: 10,
  },
  logUrl: "http://192.16.0.1/logs",
} as NodeDetail;

const WORKER: Worker = {
  cmdline: ["echo hi"],
  pid: 3456,
  cpuPercent: 14,
  memoryInfo: {
    rss: 75,
    vms: 0,
    pageins: 0,
    pfaults: 0,
  },
  coreWorkerStats: [
    {
      workerId: "worker-12345",
    } as CoreWorkerStats,
  ],
} as Worker;

const DEAD_NODE = { ...NODE, state: "DEAD" };

describe("NodeRow", () => {
  it("renders", async () => {
    render(
      <NodeRow
        node={NODE}
        expanded
        onExpandButtonClick={() => {
          /* purposefully empty */
        }}
      />,
      {
        wrapper: ({ children }) => (
          <MemoryRouter>
            <table>
              <tbody>{children}</tbody>
            </table>
          </MemoryRouter>
        ),
      },
    );

    await screen.findByText("test-hostname");
    expect(screen.getByText(/ALIVE/)).toBeVisible();
    expect(screen.getByText(/1234567890ab/)).toBeVisible();
    expect(screen.getByText(/192\.168\.0\.1.*\(Head\)/)).toBeVisible();
    // CPU Usage
    expect(screen.getByText(/15%/)).toBeVisible();
    // Memory Usage
    expect(screen.getByText(/5\.0000B\/100\.0000B\(5\.0%\)/)).toBeVisible();
    // Disk Usage
    expect(screen.getByText(/19\.07MB\/190\.73MB\(10\.0%\)/)).toBeVisible();
    // Object store memory
    expect(screen.getByText(/10\.0000B\/50\.0000B\(20\.0%\)/)).toBeVisible();
    // Network usage
    expect(screen.getByText(/5.0000B\/s/)).toBeVisible();
    expect(screen.getByText(/10.0000B\/s/)).toBeVisible();
  });

  it("Disable actions for Dead node", async () => {
    render(
      <NodeRow node={DEAD_NODE} expanded={false} onExpandButtonClick={noop} />,
      {
        wrapper: ({ children }) => (
          <MemoryRouter>
            <table>
              <tbody>{children}</tbody>
            </table>
          </MemoryRouter>
        ),
      },
    );
    await screen.findByText("test-hostname");
    // Could not access logs for Dead nodes(the log is hidden)
    expect(screen.queryByLabelText(/Log/)).not.toBeInTheDocument();

    expect(screen.getByText(/3456/)).toBeVisible();
  });
});

describe("WorkerRow", () => {
  it("renders", async () => {
    render(<WorkerRow node={NODE} worker={WORKER} />, {
      wrapper: ({ children }) => (
        <MemoryRouter>
          <table>
            <tbody>{children}</tbody>
          </table>
        </MemoryRouter>
      ),
    });

    await screen.findByText("echo hi");
    expect(screen.getByText(/ALIVE/)).toBeVisible();
    expect(screen.getByText(/worker-12345/)).toBeVisible();
    expect(screen.getByText(/3456/)).toBeVisible();
    // CPU Usage
    expect(screen.getByText(/14%/)).toBeVisible();
    // Memory Usage
    expect(screen.getByText(/75\.0000B\/100\.0000B\(75\.0%\)/)).toBeVisible();
  });
});
