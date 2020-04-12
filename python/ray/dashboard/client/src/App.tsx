import { CssBaseline } from "@material-ui/core";
import React from "react";
import { Provider } from "react-redux";
import { BrowserRouter, Route } from "react-router-dom";
import Dashboard from "./pages/dashboard/Dashboard";
import { store } from "./store";

class App extends React.Component {
  render() {
    return (
      <Provider store={store}>
        <BrowserRouter>
          <CssBaseline />
          <Route component={Dashboard} exact path="/" />
        </BrowserRouter>
      </Provider>
    );
  }
}

export default App;
