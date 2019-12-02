import React from "react";
import { NodeFeatureComponent, WorkerFeatureComponent } from "./types";

export const NodeWorkers: NodeFeatureComponent = ({ node }) => (
  <React.Fragment>{node.workers.length}</React.Fragment>
);

export const WorkerWorkers: WorkerFeatureComponent = ({ worker }) => (
  <React.Fragment>{worker.cmdline[0].split("::", 2)[1]}</React.Fragment>
);
