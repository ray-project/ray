import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import {
  NodeInfoResponse,
  RayConfigResponse,
  RayletInfoResponse
} from "../../api";

const name = "dashboard";

interface State {
  tab: number;
  rayConfig: RayConfigResponse | null;
  nodeInfo: NodeInfoResponse | null;
  rayletInfo: RayletInfoResponse | null;
  lastUpdatedAt: number | null;
  error: string | null;
}

const initialState: State = {
  tab: 0,
  rayConfig: null,
  nodeInfo: null,
  rayletInfo: null,
  lastUpdatedAt: null,
  error: null
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
      }>
    ) => {
      state.nodeInfo = action.payload.nodeInfo;
      state.rayletInfo = action.payload.rayletInfo;
      state.lastUpdatedAt = Date.now();
    },
    setError: (state, action: PayloadAction<string | null>) => {
      state.error = action.payload;
    }
  }
});

export const dashboardActions = slice.actions;
export const dashboardReducer = slice.reducer;
