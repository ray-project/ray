import React from "react";
import {
  ClusterFeature,
  NodeFeature,
  NodeInfoFeature,
  WorkerFeature,
} from "./types";

export const ClusterWorkers: ClusterFeature = ({ nodes }) => {
  let totalCpus = 0;
  let totalWorkers = 0;
  for (const node of nodes) {
    totalCpus += node.cpus[0];
    totalWorkers += node.workers.length;
  }
  return (
    <React.Fragment>
      {totalWorkers.toLocaleString()}{" "}
      {totalWorkers === 1 ? "worker" : "workers"} / {totalCpus.toLocaleString()}{" "}
      {totalCpus === 1 ? "core" : "cores"}
    </React.Fragment>
  );
};

export const NodeWorkers: NodeFeature = ({ node }) => {
  const cpus = node.cpus[0];
  const totalWorkers = node.workers.length;
  return (
    <React.Fragment>
      {totalWorkers.toLocaleString()}{" "}
      {totalWorkers === 1 ? "worker" : "workers"} / {cpus.toLocaleString()}{" "}
      {cpus === 1 ? "core" : "cores"}
    </React.Fragment>
  );
};

// Ray worker process titles have one of the following forms: `ray::IDLE`,
// `ray::function()`, `ray::Class`, or `ray::Class.method()`. We extract the
// second portion here for display in the "Workers" column.
export const WorkerWorkers: WorkerFeature = ({ worker }) => (
  <React.Fragment>{worker.cmdline[0].split("::", 2)[1]}</React.Fragment>
);

const workersFeature: NodeInfoFeature = {
  id: "workers",
  ClusterFeature: ClusterWorkers,
  NodeFeature: NodeWorkers,
  WorkerFeature: WorkerWorkers,
};

export default workersFeature;
