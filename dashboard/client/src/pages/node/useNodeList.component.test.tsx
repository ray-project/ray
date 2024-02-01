import { render, screen, within } from "@testing-library/react";
import { noop } from "lodash";
import React from "react";
import { MemoryRouter } from "react-router-dom";
import { NodeDetail } from "../../type/node";
import { CoreWorkerStats, Worker } from "../../type/worker";
import { NodeRow, WorkerRow } from "./NodeRow";
import { useNodeList } from "./hook/useNodeList";
import useSWR from "swr";

jest.mock("swr");
const useSWRMocked = jest.mocked(useSWR);

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
  } as NodeDetail;

  const aliveHeadNode = { ...NODE, hostname: "test-hostname-alive-head-node"};
  const deadHeadNode = { ...NODE, hostname: "test-hostname-dead-head-node", state: "DEAD", raylet: {
    state: "DEAD",
    nodeId: "1234567890ab",
    isHeadNode: false,
    numWorkers: 0,
    pid: 2345,
    startTime: 100,
    terminateTime: -1,
    brpcPort: 3456,
    nodeManagerPort: 5890,
    objectStoreAvailableMemory: 40,
    objectStoreUsedMemory: 10,
  },};
  const aliveWorkerNode1 = { ...NODE, hostname: "test-hostname-worker1", raylet: {
    state: "ALIVE",
    nodeId: "1234567890ab",
    isHeadNode: false,
    numWorkers: 0,
    pid: 2345,
    startTime: 100,
    terminateTime: -1,
    brpcPort: 3456,
    nodeManagerPort: 5890,
    objectStoreAvailableMemory: 40,
    objectStoreUsedMemory: 10,
  },};
  const aliveWorkerNode2 = { ...NODE, hostname: "test-hostname-worker2", raylet: {
    state: "ALIVE",
    nodeId: "1234567890ac",
    isHeadNode: false,
    numWorkers: 0,
    pid: 2345,
    startTime: 100,
    terminateTime: -1,
    brpcPort: 3456,
    nodeManagerPort: 5890,
    objectStoreAvailableMemory: 40,
    objectStoreUsedMemory: 10,
  },};

describe("useNodeList", () => {
    it("verify default sort order of useNodeList", () => {
        useSWRMocked.mockReturnValue({
            data: {
                summary: [deadHeadNode, aliveWorkerNode2, aliveHeadNode, aliveWorkerNode1,],
                nodeLogicalResources: undefined,
            },
            isLoading: false,
          } as any);

          function TestComponent() {
            const {
                msg,
                isLoading,
                isRefreshing,
                onSwitchChange,
                nodeList,
                changeFilter,
                page,
                setPage,
                setSortKey,
                setOrderDesc,
                mode,
                setMode,
              } = useNodeList();
            const nodeHostNames = nodeList
              .map((e) => (e.hostname))
            return <div data-testid="nodeHostNames">{nodeHostNames}</div>;
          }

          const { getByRole } = render(
            <MemoryRouter>
              <TestComponent />
            </MemoryRouter>,
          );
          screen.debug(undefined, Infinity);
          screen.logTestingPlaygroundURL()

          const nodeHostNames = screen.getByTestId("nodeHostNames");
          const expectedOrderNodeList = [aliveHeadNode.hostname, aliveWorkerNode1.hostname, aliveWorkerNode2.hostname, deadHeadNode.hostname,];
          expect(nodeHostNames.textContent).toEqual(expectedOrderNodeList.join(""));
    })
})