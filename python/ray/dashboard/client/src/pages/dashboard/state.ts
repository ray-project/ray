import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { NodeInfoResponse, RayConfigResponse } from "../../api";

const name = "dashboard";

interface State {
  rayConfig: RayConfigResponse | null;
  nodeInfo: NodeInfoResponse | null;
  lastUpdatedAt: number | null;
  error: string | null;
}

const initialState: State = {
  rayConfig: null,
  nodeInfo: null,
  lastUpdatedAt: null,
  error: null
};

const slice = createSlice({
  name,
  initialState,
  reducers: {
    setRayConfig: (state, action: PayloadAction<RayConfigResponse>) => {
      state.rayConfig = action.payload;
    },
    setNodeInfo: (state, action: PayloadAction<NodeInfoResponse>) => {
      state.nodeInfo = action.payload;
      state.lastUpdatedAt = Date.now();
    },
    setError: (state, action: PayloadAction<string | null>) => {
      state.error = action.payload;
    }
  }
});

export const dashboardActions = slice.actions;
export const dashboardReducer = slice.reducer;
