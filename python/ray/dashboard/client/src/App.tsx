import CssBaseline from "@material-ui/core/CssBaseline";
import React from "react";
import { BrowserRouter } from "react-router-dom";
import Dashboard from "./Dashboard";

class App extends React.Component {
  render() {
    return (
      <BrowserRouter>
        <CssBaseline />
        <Dashboard />
      </BrowserRouter>
    );
  }
}

export default App;
