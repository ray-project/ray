import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { NodeInfoResponse } from "../../api";

const name = "dashboard";

interface State {
  nodeInfo: NodeInfoResponse | null;
  lastUpdatedAt: number | null;
  error: string | null;
}

const initialState: State = {
  nodeInfo: null,
  lastUpdatedAt: null,
  error: null
};

const slice = createSlice({
  name,
  initialState,
  reducers: {
    setNodeInfo: (state, action: PayloadAction<NodeInfoResponse>) => {
      state.nodeInfo = action.payload;
      state.lastUpdatedAt = Date.now();
    },
    setError: (state, action: PayloadAction<string>) => {
      state.error = action.payload;
    }
  }
});

export const dashboardActions = slice.actions;
export const dashboardReducer = slice.reducer;
