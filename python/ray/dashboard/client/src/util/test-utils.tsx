import { StyledEngineProvider, ThemeProvider } from "@mui/material/styles";
import React, { PropsWithChildren } from "react";
import { MemoryRouter } from "react-router-dom";
import { SWRConfig } from "swr";
import { GlobalContext, GlobalContextType } from "../App";
import { ThemeProvider as CustomThemeProvider, useTheme } from "../contexts/ThemeContext";
import { darkTheme, lightTheme } from "../theme";

const ThemedWrapper = ({ children }: PropsWithChildren<{}>) => {
  const { mode } = useTheme();
  const currentTheme = mode === 'dark' ? darkTheme : lightTheme;
  
  return (
    <ThemeProvider theme={currentTheme}>
      {children}
    </ThemeProvider>
  );
};

export const TEST_APP_WRAPPER = ({ children }: PropsWithChildren<{}>) => {
  const context: GlobalContextType = {
    nodeMap: {},
    nodeMapByIp: {},
    namespaceMap: {},
    metricsContextLoaded: true,
    grafanaHost: "localhost:3000",
    grafanaOrgId: "1",
    dashboardUids: {
      default: "rayDefaultDashboard",
      serve: "rayServeDashboard",
      serveDeployment: "rayServeDeploymentDashboard",
      data: "rayDataDashboard",
    },
    prometheusHealth: true,
    sessionName: "session-name",
    dashboardDatasource: "Prometheus",
    serverTimeZone: undefined,
    currentTimeZone: undefined,
  };

  return (
    <STYLE_WRAPPER>
      {/*
          Clear SWR cache between tests so that tests do impact each other.
        */}
      <SWRConfig value={{ provider: () => new Map() }}>
        <CustomThemeProvider>
          <GlobalContext.Provider value={context}>
            <MemoryRouter>{children}</MemoryRouter>
          </GlobalContext.Provider>
        </CustomThemeProvider>
      </SWRConfig>
    </STYLE_WRAPPER>
  );
};

export const STYLE_WRAPPER = ({ children }: PropsWithChildren<{}>) => {
  return (
    <StyledEngineProvider injectFirst>
      <CustomThemeProvider>
        <ThemedWrapper>{children}</ThemedWrapper>
      </CustomThemeProvider>
    </StyledEngineProvider>
  );
};
