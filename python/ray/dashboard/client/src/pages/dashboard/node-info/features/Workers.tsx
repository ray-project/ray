import React from "react";
import {
  ClusterFeatureComponent,
  NodeFeatureComponent,
  WorkerFeatureComponent,
} from "./types";

export const ClusterWorkers: ClusterFeatureComponent = ({ nodes }) => {
  let totalWorkers = 0;
  let totalCpus = 0;
  for (const node of nodes) {
    totalWorkers += node.workers.length;
    totalCpus += node.cpus[0];
  }
  return (
    <React.Fragment>
      {totalWorkers.toLocaleString()}{" "}
      {totalWorkers === 1 ? "worker" : "workers"} / {totalCpus.toLocaleString()}{" "}
      {totalCpus === 1 ? "core" : "cores"}
    </React.Fragment>
  );
};

export const NodeWorkers: NodeFeatureComponent = ({ node }) => {
  const workers = node.workers.length;
  const cpus = node.cpus[0];
  return (
    <React.Fragment>
      {workers.toLocaleString()} {workers === 1 ? "worker" : "workers"} /{" "}
      {cpus.toLocaleString()} {cpus === 1 ? "core" : "cores"}
    </React.Fragment>
  );
};

// Ray worker process titles have one of the following forms: `ray::IDLE`,
// `ray::function()`, `ray::Class`, or `ray::Class.method()`. We extract the
// second portion here for display in the "Workers" column.
export const WorkerWorkers: WorkerFeatureComponent = ({ worker }) => (
  <React.Fragment>{worker.cmdline[0].split("::", 2)[1]}</React.Fragment>
);
