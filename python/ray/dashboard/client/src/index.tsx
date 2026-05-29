import React from "react";
import { createRoot } from "react-dom/client";
import "typeface-roboto";
import App from "./App";

const rootElement = document.getElementById("root");
if (rootElement !== null) {
  const root = createRoot(rootElement);
  root.render(<App />);
} else {
  console.error("Could not find root element.");
}
