import React from "react";
import { NodeInfoResponse } from "../../../../api";
import { Accessor } from "../../../../common/tableUtils";

type ArrayType<T> = T extends Array<infer U> ? U : never;
export type Node = ArrayType<NodeInfoResponse["clients"]>;
export type Worker = ArrayType<Node["workers"]>;

type ClusterFeatureData = { nodes: Node[] };
export type NodeFeatureData = { node: Node };
export type WorkerFeatureData = {
  node: Node;
  worker: Worker;
};

export type ClusterFeatureRenderFn = (
  data: ClusterFeatureData,
) => React.ReactElement;
export type NodeFeatureRenderFn = (data: NodeFeatureData) => React.ReactElement;
export type WorkerFeatureRenderFn = (
  data: WorkerFeatureData,
) => React.ReactElement;

export type NodeInfoFeature = {
  id: nodeInfoColumnId;
  WorkerFeatureRenderFn: WorkerFeatureRenderFn;
  NodeFeatureRenderFn: NodeFeatureRenderFn;
  ClusterFeatureRenderFn?: ClusterFeatureRenderFn;
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
  | "objectStoreMemory"
  | "disk"
  | "sent"
  | "received"
  | "logs"
  | "errors";
