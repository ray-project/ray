import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import {
  ActorsResponse,
  MemoryTableResponse,
  NodeInfoResponse,
  RayConfigResponse,
  TuneAvailabilityResponse,
  TuneJobResponse,
} from "../../api";

const name = "dashboard";

type State = {
  tab: number;
  rayConfig: RayConfigResponse | null;
  nodeInfo: NodeInfoResponse | null;
  actors: ActorsResponse | null;
  tuneInfo: TuneJobResponse | null;
  tuneAvailability: TuneAvailabilityResponse | null;
  lastUpdatedAt: number | null;
  error: string | null;
  memoryTable: MemoryTableResponse | null;
  shouldObtainMemoryTable: boolean;
};

const initialState: State = {
  actors: null,
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
    setActorInfo: (
      state,
      action: PayloadAction<{
        actorsResponse: ActorsResponse;
      }>
    ) => {
      state.actors = action.payload.actorsResponse;
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

export const dashboardActions = slice.actions;
export const dashboardReducer = slice.reducer;
