import { CssBaseline } from "@material-ui/core";
import { ThemeProvider } from "@material-ui/core/styles";
import dayjs from "dayjs";
import duration from "dayjs/plugin/duration";
import React, { Suspense, useEffect, useState } from "react";
import { HashRouter, Navigate, Route, Routes } from "react-router-dom";
import Events from "./pages/event/Events";
import Loading from "./pages/exception/Loading";
import JobList, { NewIAJobsPage } from "./pages/job";
import { JobDetailChartsPage } from "./pages/job/JobDetail";
import { JobDetailInfoPage } from "./pages/job/JobDetailInfoPage";
import { JobDetailLayout } from "./pages/job/JobDetailLayout";
import { DEFAULT_VALUE, MainNavContext } from "./pages/layout/mainNavContext";
import { MainNavLayout } from "./pages/layout/MainNavLayout";
import { SideTabPage } from "./pages/layout/SideTabLayout";
import { NewIALogsPage } from "./pages/log/Logs";
import { Metrics } from "./pages/metrics";
import { getMetricsInfo } from "./pages/metrics/utils";
import Nodes, { NewIAClusterPage } from "./pages/node";
import { ClusterDetailInfoPage } from "./pages/node/ClusterDetailInfoPage";
import { ClusterLayout } from "./pages/node/ClusterLayout";
import NodeDetailPage from "./pages/node/NodeDetail";
import { OverviewPage } from "./pages/overview/OverviewPage";
import { getNodeList } from "./service/node";
import { darkTheme, lightTheme } from "./theme";
import { getLocalStorage, setLocalStorage } from "./util/localData";

dayjs.extend(duration);

// lazy loading fro prevent loading too much code at once
const Actors = React.lazy(() => import("./pages/actor"));
const CMDResult = React.lazy(() => import("./pages/cmd/CMDResult"));
const Index = React.lazy(() => import("./pages/index/Index"));
const Job = React.lazy(() => import("./pages/job"));
const BasicLayout = React.lazy(() => import("./pages/layout"));
const Logs = React.lazy(() => import("./pages/log/Logs"));
const Node = React.lazy(() => import("./pages/node"));
const NodeDetail = React.lazy(() => import("./pages/node/NodeDetail"));

// key to store theme in local storage
const RAY_DASHBOARD_THEME_KEY = "ray-dashboard-theme";

// a global map for relations
type GlobalContextType = {
  nodeMap: { [key: string]: string };
  nodeMapByIp: { [key: string]: string };
  ipLogMap: { [key: string]: string };
  namespaceMap: { [key: string]: string[] };
  /**
   * The host that is serving grafana. Only set if grafana is
   * running as detected by the grafana healthcheck endpoint.
   */
  grafanaHost: string | undefined;
  /**
   * The name of the currently running ray session.
   */
  sessionName: string | undefined;
};
export const GlobalContext = React.createContext<GlobalContextType>({
  nodeMap: {},
  nodeMapByIp: {},
  ipLogMap: {},
  namespaceMap: {},
  grafanaHost: undefined,
  sessionName: undefined,
});

export const getDefaultTheme = () =>
  getLocalStorage<string>(RAY_DASHBOARD_THEME_KEY) || "light";
export const setLocalTheme = (theme: string) =>
  setLocalStorage(RAY_DASHBOARD_THEME_KEY, theme);

const App = () => {
  const [theme, _setTheme] = useState(getDefaultTheme());
  const [context, setContext] = useState<GlobalContextType>({
    nodeMap: {},
    nodeMapByIp: {},
    ipLogMap: {},
    namespaceMap: {},
    grafanaHost: undefined,
    sessionName: undefined,
  });
  const getTheme = (name: string) => {
    switch (name) {
      case "dark":
        return darkTheme;
      case "light":
      default:
        return lightTheme;
    }
  };
  const setTheme = (name: string) => {
    setLocalTheme(name);
    _setTheme(name);
  };
  useEffect(() => {
    getNodeList().then((res) => {
      if (res?.data?.data?.summary) {
        const nodeMap = {} as { [key: string]: string };
        const nodeMapByIp = {} as { [key: string]: string };
        const ipLogMap = {} as { [key: string]: string };
        res.data.data.summary.forEach(({ hostname, raylet, ip, logUrl }) => {
          nodeMap[hostname] = raylet.nodeId;
          nodeMapByIp[ip] = raylet.nodeId;
          ipLogMap[ip] = logUrl;
        });
        setContext((existingContext) => ({
          ...existingContext,
          nodeMap,
          nodeMapByIp,
          ipLogMap,
          namespaceMap: {},
        }));
      }
    });
  }, []);

  // Detect if grafana is running
  useEffect(() => {
    const doEffect = async () => {
      const { grafanaHost, sessionName } = await getMetricsInfo();
      setContext((existingContext) => ({
        ...existingContext,
        grafanaHost,
        sessionName,
      }));
    };
    doEffect();
  }, []);

  return (
    <ThemeProvider theme={getTheme(theme)}>
      <Suspense fallback={Loading}>
        <GlobalContext.Provider value={context}>
          <CssBaseline />
          <HashRouter>
            {/* Dummy MainNavContext so we can re-use existing pages in new layout */}
            <MainNavContext.Provider value={DEFAULT_VALUE}>
              <Routes>
                <Route element={<Navigate replace to="/node" />} path="/" />
                <Route
                  element={<BasicLayout setTheme={setTheme} theme={theme} />}
                >
                  <Route element={<Index />} path="/summary" />
                  <Route element={<Job />} path="/job" />
                  <Route element={<Node />} path="/node" />
                  <Route element={<Actors />} path="/actors" />
                  <Route element={<Events />} path="/events" />
                  <Route element={<Metrics />} path="/metrics" />
                  {/* TODO(aguo): Refactor Logs component to use optional query
                      params since react-router 6 doesn't support optional path params... */}
                  <Route
                    element={<Logs theme={theme as "light" | "dark"} />}
                    path="/log"
                  />
                  <Route
                    element={<Logs theme={theme as "light" | "dark"} />}
                    path="/log/:host"
                  />
                  <Route
                    element={<Logs theme={theme as "light" | "dark"} />}
                    path="/log/:host/:path"
                  />
                  <Route element={<NodeDetail />} path="/node/:id" />
                  <Route element={<JobDetailChartsPage />} path="/job/:id" />
                  <Route element={<CMDResult />} path="/cmd/:cmd/:ip/:pid" />
                  <Route element={<Loading />} path="/loading" />
                </Route>
                {/* New IA routes below! */}
                <Route element={<MainNavLayout />} path="/new">
                  <Route element={<Navigate replace to="overview" />} path="" />
                  <Route element={<OverviewPage />} path="overview" />
                  <Route element={<NewIAClusterPage />} path="cluster">
                    <Route element={<ClusterLayout />} path="">
                      <Route
                        element={
                          <SideTabPage tabId="info">
                            <ClusterDetailInfoPage />
                          </SideTabPage>
                        }
                        path="info"
                      />
                      <Route
                        element={
                          <SideTabPage tabId="table">
                            <Nodes newIA />
                          </SideTabPage>
                        }
                        path=""
                      />
                      <Route element={<NodeDetailPage />} path="nodes/:id" />
                    </Route>
                  </Route>
                  <Route element={<NewIAJobsPage />} path="jobs">
                    <Route element={<JobList newIA />} path="" />
                    <Route element={<JobDetailLayout />} path=":id">
                      <Route
                        element={
                          <SideTabPage tabId="info">
                            <JobDetailInfoPage />
                          </SideTabPage>
                        }
                        path="info"
                      />
                      <Route
                        element={
                          <SideTabPage tabId="charts">
                            <JobDetailChartsPage />
                          </SideTabPage>
                        }
                        path=""
                      />
                    </Route>
                  </Route>
                  <Route element={<NewIALogsPage />} path="logs">
                    {/* TODO(aguo): Refactor Logs component to use optional query
                        params since react-router 6 doesn't support optional path params... */}
                    <Route element={<Logs newIA />} path="" />
                    <Route element={<Logs newIA />} path=":host">
                      <Route element={<Logs newIA />} path=":path" />
                    </Route>
                  </Route>
                </Route>
              </Routes>
            </MainNavContext.Provider>
          </HashRouter>
        </GlobalContext.Provider>
      </Suspense>
    </ThemeProvider>
  );
};

export default App;
