import { ThemeProvider } from "@material-ui/styles";
import React, { PropsWithChildren } from "react";
import { MemoryRouter } from "react-router-dom";
import { SWRConfig } from "swr";
import { GlobalContext, GlobalContextType } from "../App";
import { lightTheme } from "../theme";

export const TEST_APP_WRAPPER = ({ children }: PropsWithChildren<{}>) => {
  const context: GlobalContextType = {
    nodeMap: {},
    nodeMapByIp: {},
    ipLogMap: {},
    namespaceMap: {},
    metricsContextLoaded: true,
    grafanaHost: "localhost:3000",
    dashboardUids: {
      default: "rayDefaultDashboard",
      serve: "rayServeDashboard",
      serveDeployment: "rayServeDeploymentDashboard",
    },
    prometheusHealth: true,
    sessionName: "session-name",
  };

  return (
    <ThemeProvider theme={lightTheme}>
      {/*
        Clear SWR cache between tests so that tests do impact each other.
      */}
      <SWRConfig value={{ provider: () => new Map() }}>
        <GlobalContext.Provider value={context}>
          <MemoryRouter>{children}</MemoryRouter>
        </GlobalContext.Provider>
      </SWRConfig>
    </ThemeProvider>
  );
};
