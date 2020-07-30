import React from "react";
import { Accessor } from "../../../../common/tableUtils";
import {
  ClusterFeature,
  NodeFeature,
  NodeFeatureData,
  NodeInfoFeature,
  WorkerFeature,
} from "./types";

export const ClusterHost: ClusterFeature = ({ nodes }) => (
  <React.Fragment>
    Totals ({nodes.length.toLocaleString()}{" "}
    {nodes.length === 1 ? "host" : "hosts"})
  </React.Fragment>
);

export const NodeHost: NodeFeature = ({ node }) => (
  <React.Fragment>
    {node.hostname} ({node.ip})
  </React.Fragment>
);

export const nodeHostAccessor: Accessor<NodeFeatureData> = ({ node }) =>
  node.hostname;

// Ray worker process titles have one of the following forms: `ray::IDLE`,
// `ray::function()`, `ray::Class`, or `ray::Class.method()`. We extract the
// first portion here for display in the "Host" column. Note that this will
// always be `ray` under the current setup, but it may vary in the future.
export const WorkerHost: WorkerFeature = ({ worker }) => (
  <React.Fragment>
    {worker.cmdline[0].split("::", 2)[0]} (PID: {worker.pid})
  </React.Fragment>
);

const hostFeature: NodeInfoFeature = {
  id: "host",
  ClusterFeature: ClusterHost,
  NodeFeature: NodeHost,
  WorkerFeature: WorkerHost,
  nodeAccessor: nodeHostAccessor,
};

export default hostFeature;
