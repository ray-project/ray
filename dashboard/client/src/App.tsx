import { CssBaseline } from "@material-ui/core";
import { ThemeProvider } from "@material-ui/core/styles";
import dayjs from "dayjs";
import duration from "dayjs/plugin/duration";
import React, { Suspense, useEffect, useState } from "react";
import { HashRouter, Navigate, Route, Routes } from "react-router-dom";
import ActorDetailPage from "./pages/actor/ActorDetail";
import Loading from "./pages/exception/Loading";
import JobList, { JobsLayout } from "./pages/job";
import { JobDetailChartsPage } from "./pages/job/JobDetail";
import { JobDetailActorsPage } from "./pages/job/JobDetailActorPage";
import { JobDetailInfoPage } from "./pages/job/JobDetailInfoPage";
import { JobDetailLayout } from "./pages/job/JobDetailLayout";
import { MainNavLayout } from "./pages/layout/MainNavLayout";
import { SideTabPage } from "./pages/layout/SideTabLayout";
import { LogsLayout } from "./pages/log/Logs";
import { Metrics } from "./pages/metrics";
import { getMetricsInfo } from "./pages/metrics/utils";
import Nodes, { ClusterMainPageLayout } from "./pages/node";
import { ClusterDetailInfoPage } from "./pages/node/ClusterDetailInfoPage";
import { ClusterLayout } from "./pages/node/ClusterLayout";
import NodeDetailPage from "./pages/node/NodeDetail";
import { OverviewPage } from "./pages/overview/OverviewPage";
import { getNodeList } from "./service/node";
import { lightTheme } from "./theme";

dayjs.extend(duration);

// lazy loading fro prevent loading too much code at once
const Actors = React.lazy(() => import("./pages/actor"));
const CMDResult = React.lazy(() => import("./pages/cmd/CMDResult"));
const Logs = React.lazy(() => import("./pages/log/Logs"));

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
   * The uid of the default dashboard that powers the Metrics page.
   */
  grafanaDefaultDashboardUid: string | undefined;
  /**
   * Whether prometheus is runing or not
   */
  prometheusHealth: boolean | undefined;
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
  grafanaDefaultDashboardUid: undefined,
  prometheusHealth: undefined,
  sessionName: undefined,
});

const App = () => {
  const [context, setContext] = useState<GlobalContextType>({
    nodeMap: {},
    nodeMapByIp: {},
    ipLogMap: {},
    namespaceMap: {},
    grafanaHost: undefined,
    grafanaDefaultDashboardUid: undefined,
    prometheusHealth: undefined,
    sessionName: undefined,
  });
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
      const {
        grafanaHost,
        sessionName,
        prometheusHealth,
        grafanaDefaultDashboardUid,
      } = await getMetricsInfo();
      setContext((existingContext) => ({
        ...existingContext,
        grafanaHost,
        grafanaDefaultDashboardUid,
        sessionName,
        prometheusHealth,
      }));
    };
    doEffect();
  }, []);

  return (
    <ThemeProvider theme={lightTheme}>
      <Suspense fallback={Loading}>
        <GlobalContext.Provider value={context}>
          <CssBaseline />
          <HashRouter>
            <Routes>
              {/* Redirect people hitting the /new path to root. TODO(aguo): Delete this redirect in ray 2.5 */}
              <Route element={<Navigate replace to="/" />} path="/new" />
              <Route element={<MainNavLayout />} path="/">
                <Route element={<Navigate replace to="overview" />} path="" />
                <Route element={<OverviewPage />} path="overview" />
                <Route element={<ClusterMainPageLayout />} path="cluster">
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
                          <Nodes />
                        </SideTabPage>
                      }
                      path=""
                    />
                    <Route element={<NodeDetailPage />} path="nodes/:id" />
                  </Route>
                </Route>
                <Route element={<JobsLayout />} path="jobs">
                  <Route element={<JobList />} path="" />
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
                    <Route
                      element={
                        <SideTabPage tabId="actors">
                          <JobDetailActorsPage />
                        </SideTabPage>
                      }
                      path="actors"
                    />
                    <Route element={<ActorDetailPage />} path="actors/:id" />
                  </Route>
                </Route>
                <Route element={<Actors />} path="actors" />
                <Route element={<ActorDetailPage />} path="actors/:id" />
                <Route element={<Metrics />} path="metrics" />
                <Route element={<LogsLayout />} path="logs">
                  {/* TODO(aguo): Refactor Logs component to use optional query
                        params since react-router 6 doesn't support optional path params... */}
                  <Route element={<Logs />} path="" />
                  <Route element={<Logs />} path=":host">
                    <Route element={<Logs />} path=":path" />
                  </Route>
                </Route>
              </Route>
              <Route element={<CMDResult />} path="/cmd/:cmd/:ip/:pid" />
            </Routes>
          </HashRouter>
        </GlobalContext.Provider>
      </Suspense>
    </ThemeProvider>
  );
};

export default App;
