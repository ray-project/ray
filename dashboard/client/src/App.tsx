import { CssBaseline } from "@material-ui/core";
import { ThemeProvider } from "@material-ui/core/styles";
import dayjs from "dayjs";
import duration from "dayjs/plugin/duration";
import React, { Suspense, useEffect, useState } from "react";
import { Provider } from "react-redux";
import { HashRouter, Redirect, Route, Switch } from "react-router-dom";
import Events from "./pages/event/Events";
import Loading from "./pages/exception/Loading";
import { Metrics } from "./pages/metrics";
import { getGrafanaHost } from "./pages/metrics/utils";
import { getNodeList } from "./service/node";
import { store } from "./store";
import { darkTheme, lightTheme } from "./theme";
import { getLocalStorage, setLocalStorage } from "./util/localData";

dayjs.extend(duration);

// lazy loading fro prevent loading too much code at once
const Actors = React.lazy(() => import("./pages/actor"));
const CMDResult = React.lazy(() => import("./pages/cmd/CMDResult"));
const Index = React.lazy(() => import("./pages/index/Index"));
const Job = React.lazy(() => import("./pages/job"));
const JobDetail = React.lazy(() => import("./pages/job/JobDetail"));
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
};
export const GlobalContext = React.createContext<GlobalContextType>({
  nodeMap: {},
  nodeMapByIp: {},
  ipLogMap: {},
  namespaceMap: {},
  grafanaHost: undefined,
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
      const grafanaHost = await getGrafanaHost();
      setContext((existingContext) => ({
        ...existingContext,
        grafanaHost,
      }));
    };
    doEffect();
  }, []);

  return (
    <ThemeProvider theme={getTheme(theme)}>
      <Suspense fallback={Loading}>
        <GlobalContext.Provider value={context}>
          <Provider store={store}>
            <CssBaseline />
            <HashRouter>
              <Switch>
                <Route
                  component={() => <Redirect to="/node" />}
                  exact
                  path="/"
                />
                <Route
                  render={(props) => (
                    <BasicLayout {...props} setTheme={setTheme} theme={theme}>
                      <Route component={Index} exact path="/summary" />
                      <Route component={Job} exact path="/job" />
                      <Route component={Node} exact path="/node" />
                      <Route component={Actors} exact path="/actors" />
                      <Route component={Events} exact path="/events" />
                      <Route component={Metrics} exact path="/metrics" />
                      <Route
                        render={(props) => (
                          <Logs {...props} theme={theme as "light" | "dark"} />
                        )}
                        exact
                        path="/log/:host?/:path?"
                      />
                      <Route component={NodeDetail} path="/node/:id" />
                      <Route component={JobDetail} path="/job/:id" />
                      <Route component={CMDResult} path="/cmd/:cmd/:ip/:pid" />
                      <Route component={Loading} exact path="/loading" />
                    </BasicLayout>
                  )}
                />
              </Switch>
            </HashRouter>
          </Provider>
        </GlobalContext.Provider>
      </Suspense>
    </ThemeProvider>
  );
};

export default App;
