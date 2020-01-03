import CssBaseline from "@material-ui/core/CssBaseline";
import React from "react";
import { Provider } from "react-redux";
import { BrowserRouter, Route } from "react-router-dom";
import Dashboard from "./pages/dashboard/Dashboard";
import Errors from "./pages/dashboard/dialogs/errors/Errors";
import Logs from "./pages/dashboard/dialogs/logs/Logs";
import { store } from "./store";

class App extends React.Component {
  render() {
    return (
      <Provider store={store}>
        <BrowserRouter>
          <CssBaseline />
          <Dashboard />
          <Route component={Logs} path="/logs/:hostname/:pid?" />
          <Route component={Errors} path="/errors/:hostname/:pid?" />
        </BrowserRouter>
      </Provider>
    );
  }
}

export default App;
