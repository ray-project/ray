import { CssBaseline } from "@material-ui/core";
import React from "react";
import { Provider } from "react-redux";
import { HashRouter, Route } from "react-router-dom";
import Dashboard from "./pages/dashboard/Dashboard";
import { Logs } from "./pages/dashboard/log/Logs";
import NodeDetailPage from "./pages/node-detail/NodeDetail";
import { store } from "./store";

class App extends React.Component {
  render() {
    return (
      <Provider store={store}>
        <HashRouter>
          <CssBaseline />
          <Route component={Dashboard} exact path="/" />
          <Route component={NodeDetailPage} exact path="/node/:id" />
          <Route component={Logs} exact path="/log/:host?/:path?"
                  />
        </HashRouter>
      </Provider>
    );
  }
}

export default App;
