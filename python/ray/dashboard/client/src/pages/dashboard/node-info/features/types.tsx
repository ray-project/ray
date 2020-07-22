import React from "react";
import { Accessor } from "../../../../common/tableUtils";
import { NodeDetails, Worker } from "../../../../newApi";

export type ClusterFeatureData = { nodes: NodeDetails[] };
export type NodeFeatureData = { node: NodeDetails };
export type WorkerFeatureData = {
  node: NodeDetails;
  worker: Worker;
};

export type ClusterFeature = React.FC<ClusterFeatureData>;
export type NodeFeature = React.FC<NodeFeatureData>;
export type WorkerFeature = React.FC<WorkerFeatureData>;

export type NodeInfoFeature = {
  id: nodeInfoColumnId;
  WorkerFeature: WorkerFeature;
  NodeFeature: NodeFeature;
  ClusterFeature?: ClusterFeature;
  workerAccessor?: Accessor<WorkerFeatureData>;
  nodeAccessor?: Accessor<NodeFeatureData>;
};

export type nodeInfoColumnId =
  | "host"
  | "workers"
  | "uptime"
  | "cpu"
  | "ram"
  | "gpu"
  | "gram"
  | "disk"
  | "sent"
  | "received"
  | "logs"
  | "errors";
