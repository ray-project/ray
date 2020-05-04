import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import {
  NodeInfoResponse,
  RayConfigResponse,
  RayletInfoResponse,
  TuneAvailabilityResponse,
  TuneJobResponse,
} from "../../api";

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
      state.nodeInfo = action.payload.nodeInfo;
      state.rayletInfo = action.payload.rayletInfo;
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
  },
});

export const dashboardActions = slice.actions;
export const dashboardReducer = slice.reducer;
