import React from "react";
import {
  ClusterFeatureRenderFn,
  NodeFeatureRenderFn,
  NodeInfoFeature,
  WorkerFeatureRenderFn,
} from "./types";

export const ClusterWorkers: ClusterFeatureRenderFn = ({ nodes }) => {
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

export const NodeWorkers: NodeFeatureRenderFn = ({ node }) => {
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
export const WorkerWorkers: WorkerFeatureRenderFn = ({ worker }) => (
  <React.Fragment>{worker.cmdline[0].split("::", 2)[1]}</React.Fragment>
);

const workersFeature: NodeInfoFeature = {
  id: "workers",
  ClusterFeatureRenderFn: ClusterWorkers,
  NodeFeatureRenderFn: NodeWorkers,
  WorkerFeatureRenderFn: WorkerWorkers,
};

export default workersFeature;
