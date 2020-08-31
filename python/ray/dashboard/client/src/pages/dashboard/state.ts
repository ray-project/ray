import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import {
  MemoryTableResponse,
  NodeInfoResponse,
  RayConfigResponse,
  RayletInfoResponse,
  TuneAvailabilityResponse,
  TuneJobResponse,
} from "../../api";
import { filterObj } from "../../common/util";

const name = "dashboard";

type State = {
  tab: number;
  rayConfig: RayConfigResponse | null;
  nodeInfo: NodeInfoResponse | null;
  rayletInfo: RayletInfoResponse | null;
  tuneInfo: TuneJobResponse | null;
  tuneAvailability: TuneAvailabilityResponse | null;
  lastUpdatedAt: number | null;
  error: string | null;
  memoryTable: MemoryTableResponse | null;
  shouldObtainMemoryTable: boolean;
};

const initialState: State = {
  tab: 0,
  rayConfig: null,
  nodeInfo: null,
  rayletInfo: null,
  tuneInfo: null,
  tuneAvailability: null,
  lastUpdatedAt: null,
  error: null,
  memoryTable: null,
  shouldObtainMemoryTable: false,
};

const slice = createSlice({
  name,
  initialState,
  reducers: {
    setTab: (state, action: PayloadAction<number>) => {
      state.tab = action.payload;
    },
    setRayConfig: (state, action: PayloadAction<RayConfigResponse>) => {
      state.rayConfig = action.payload;
    },
    setNodeAndRayletInfo: (
      state,
      action: PayloadAction<{
        nodeInfo: NodeInfoResponse;
        rayletInfo: RayletInfoResponse;
      }>,
    ) => {
      state.rayletInfo = action.payload.rayletInfo;
      state.nodeInfo = filterNonClusterWorkerInfo(
        action.payload.rayletInfo,
        action.payload.nodeInfo,
      );
      state.lastUpdatedAt = Date.now();
    },
    setTuneInfo: (state, action: PayloadAction<TuneJobResponse>) => {
      state.tuneInfo = action.payload;
      state.lastUpdatedAt = Date.now();
    },
    setTuneAvailability: (
      state,
      action: PayloadAction<TuneAvailabilityResponse>,
    ) => {
      state.tuneAvailability = action.payload;
      state.lastUpdatedAt = Date.now();
    },
    setError: (state, action: PayloadAction<string | null>) => {
      state.error = action.payload;
    },
    setMemoryTable: (
      state,
      action: PayloadAction<MemoryTableResponse | null>,
    ) => {
      state.memoryTable = action.payload;
    },
    setShouldObtainMemoryTable: (state, action: PayloadAction<boolean>) => {
      state.shouldObtainMemoryTable = action.payload;
    },
  },
});

const clusterWorkerPids = (
  rayletInfo: RayletInfoResponse,
): Map<string, Set<number>> => {
  // Groups PIDs registered with the raylet by node IP address
  // This is used to filter out processes belonging to other ray clusters.
  const nodeMap = new Map();
  const workerPids = new Set();
  for (const [nodeIp, { workersStats }] of Object.entries(rayletInfo.nodes)) {
    for (const worker of workersStats) {
      if (!worker.isDriver) {
        workerPids.add(worker.pid);
      }
    }
    nodeMap.set(nodeIp, workerPids);
  }
  return nodeMap;
};

const filterNonClusterWorkerInfo = (
  rayletInfo: RayletInfoResponse,
  nodeInfo: NodeInfoResponse,
) => {
  // The back-end that generates the NodeInfoResponse does not remove worker
  // information of workers that belong to other clusters, so we do it here.
  const workerPidsByIP = clusterWorkerPids(rayletInfo);
  const filteredClients = nodeInfo.clients.map((client) => {
    const workerPids = workerPidsByIP.get(client.ip);
    const workers = client.workers.filter((worker) =>
      workerPids?.has(worker.pid),
    );
    const logs = client.log_count
      ? filterObj(client.log_count, ([pid, _]: [string, any]) =>
          workerPids?.has(parseInt(pid)),
        )
      : {};
    const errors = client.error_count
      ? filterObj(client.error_count, ([pid, _]: [string, any]) =>
          workerPids?.has(parseInt(pid)),
        )
      : {};
    client.workers = workers;
    client.log_count = logs;
    client.error_count = errors;
    return client;
  });
  return {
    clients: filteredClients,
  };
};

export const dashboardActions = slice.actions;
export const dashboardReducer = slice.reducer;
