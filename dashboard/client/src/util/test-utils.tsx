import { ThemeProvider } from "@material-ui/styles";
import React, { PropsWithChildren } from "react";
import { MemoryRouter } from "react-router-dom";
import { GlobalContext, GlobalContextType } from "../App";
import { lightTheme } from "../theme";

export const TEST_APP_WRAPPER = ({ children }: PropsWithChildren<{}>) => {
  const context: GlobalContextType = {
    nodeMap: {},
    nodeMapByIp: {},
    ipLogMap: {},
    namespaceMap: {},
    grafanaHost: undefined,
    grafanaDefaultDashboardUid: undefined,
    prometheusHealth: undefined,
    sessionName: undefined,
  };

  return (
    <ThemeProvider theme={lightTheme}>
      <GlobalContext.Provider value={context}>
        <MemoryRouter>{children}</MemoryRouter>
      </GlobalContext.Provider>
    </ThemeProvider>
  );
};
