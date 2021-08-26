import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import {
  ActorGroup,
  ActorsResponse,
  MemoryTable,
  MemoryTableResponse,
  NodeInfoResponse,
  RayConfigResponse,
  TuneAvailability,
  TuneAvailabilityResponse,
  TuneJob,
  TuneJobResponse,
} from "../../api";

const name = "dashboard";

type State = {
  tab: number;
  rayConfig: RayConfigResponse | null;
  nodeInfo: NodeInfoResponse | null;
  actorGroups: { [key: string]: ActorGroup } | null;
  tuneInfo: TuneJob | null;
  tuneAvailability: TuneAvailability | null;
  lastUpdatedAt: number | null;
  error: string | null;
  memoryTable: MemoryTable | null;
  shouldObtainMemoryTable: boolean;
};

const initialState: State = {
  actorGroups: null,
  tab: 0,
  rayConfig: null,
  nodeInfo: null,
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
    setNodeInfo: (
      state,
      action: PayloadAction<{
        nodeInfo: NodeInfoResponse;
      }>,
    ) => {
      state.nodeInfo = action.payload.nodeInfo;
      state.lastUpdatedAt = Date.now();
    },
    setActorGroups: (state, action: PayloadAction<ActorsResponse>) => {
      state.actorGroups = action.payload.actorGroups;
    },
    setTuneInfo: (state, action: PayloadAction<TuneJobResponse>) => {
      state.tuneInfo = action.payload.result;
      state.lastUpdatedAt = Date.now();
    },
    setTuneAvailability: (
      state,
      action: PayloadAction<TuneAvailabilityResponse>,
    ) => {
      state.tuneAvailability = action.payload.result;
      state.lastUpdatedAt = Date.now();
    },
    setError: (state, action: PayloadAction<string | null>) => {
      state.error = action.payload;
    },
    setMemoryTable: (state, action: PayloadAction<MemoryTableResponse>) => {
      state.memoryTable = action.payload.memoryTable;
    },
    setShouldObtainMemoryTable: (state, action: PayloadAction<boolean>) => {
      state.shouldObtainMemoryTable = action.payload;
    },
  },
});

export const dashboardActions = slice.actions;
export const dashboardReducer = slice.reducer;
