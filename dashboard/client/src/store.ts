import { configureStore } from "@reduxjs/toolkit";
import { dashboardReducer } from "./pages/dashboard/state";

export const store = configureStore({
  reducer: {
    dashboard: dashboardReducer,
  },
  devTools: process.env.NODE_ENV === "development",
});

export type StoreState = ReturnType<typeof store.getState>;
