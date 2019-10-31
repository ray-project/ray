import CssBaseline from "@material-ui/core/CssBaseline";
import React from "react";
import { BrowserRouter, Route } from "react-router-dom";
import Dashboard from "./Dashboard";
import Errors from "./Errors";
import Logs from "./Logs";

class App extends React.Component {
  render() {
    return (
      <BrowserRouter>
        <CssBaseline />
        <Dashboard />
        <Route component={Logs} path="/logs/:hostname/:pid?" />
        <Route component={Errors} path="/errors/:hostname/:pid?" />
      </BrowserRouter>
    );
  }
}

export default App;
