import React from "react";
import { NodeFeatureComponent, WorkerFeatureComponent } from "./types";

export const NodeWorkers: NodeFeatureComponent = ({ node }) => (
  <React.Fragment>{node.workers.length}</React.Fragment>
);

// Ray worker process titles have one of the following forms: `ray::IDLE`,
// `ray::function()`, `ray::Class`, or `ray::Class.method()`. We extract the
// second portion here for display in the "Workers" column.
export const WorkerWorkers: WorkerFeatureComponent = ({ worker }) => (
  <React.Fragment>{worker.cmdline[0].split("::", 2)[1]}</React.Fragment>
);
