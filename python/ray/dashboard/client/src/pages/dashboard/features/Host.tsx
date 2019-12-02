import React from "react";
import { NodeFeatureComponent, WorkerFeatureComponent } from "./types";

export const NodeHost: NodeFeatureComponent = ({ node }) => (
  <React.Fragment>
    {node.hostname} ({node.ip})
  </React.Fragment>
);

export const WorkerHost: WorkerFeatureComponent = ({ worker }) => (
  <React.Fragment>
    {worker.cmdline[0].split("::", 2)[0]} (PID: {worker.pid})
  </React.Fragment>
);
