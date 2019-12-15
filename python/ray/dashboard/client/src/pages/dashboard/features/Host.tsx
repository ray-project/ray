import React from "react";
import { NodeFeatureComponent, WorkerFeatureComponent } from "./types";

export const NodeHost: NodeFeatureComponent = ({ node }) => (
  <React.Fragment>
    {node.hostname} ({node.ip})
  </React.Fragment>
);

// Ray worker process titles have one of the following forms: `ray::IDLE`,
// `ray::function()`, `ray::Class`, or `ray::Class.method()`. We extract the
// first portion here for display in the "Host" column. Note that this will
// always be `ray` under the current setup, but it may vary in the future.
export const WorkerHost: WorkerFeatureComponent = ({ worker }) => (
  <React.Fragment>
    {worker.cmdline[0].split("::", 2)[0]} (PID: {worker.pid})
  </React.Fragment>
);
