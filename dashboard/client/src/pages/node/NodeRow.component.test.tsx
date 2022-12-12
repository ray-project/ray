import { render, screen } from "@testing-library/react";
import React from "react";
import { MemoryRouter } from "react-router-dom";
import { NodeDetail } from "../../type/node";
import { CoreWorkerStats, Worker } from "../../type/worker";
import { NodeRow, WorkerRow } from "./NodeRow";

describe("NodeRow", () => {
  it("renders", async () => {
    const node: NodeDetail = {
      hostname: "test-hostname",
      ip: "192.168.0.1",
      cpu: 15,
      mem: [100, 95, 5],
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
    render(
      <NodeRow
        node={node}
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
    expect(screen.getByText(/5\.0000KB\/100\.0000KB\(5\.0%\)/)).toBeVisible();
    // Disk Usage
    expect(screen.getByText(/19\.07MB\/190\.73MB\(10\.0%\)/)).toBeVisible();
    // Object store memory
    expect(screen.getByText(/10\.0000KB\/50\.0000KB\(20\.0%\)/)).toBeVisible();
    // Network usage
    expect(screen.getByText(/5.0000KB\/s/)).toBeVisible();
    expect(screen.getByText(/10.0000KB\/s/)).toBeVisible();
  });
});

describe("WorkerRow", () => {
  it("renders", async () => {
    const node: NodeDetail = {
      hostname: "test-hostname",
      ip: "192.168.0.1",
      cpu: 15,
      mem: [100, 95, 5],
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

    const worker: Worker = {
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

    render(<WorkerRow node={node} worker={worker} />, {
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
    expect(screen.getByText(/75\.0000KB\/100\.0000KB\(75\.0%\)/)).toBeVisible();
  });
});
