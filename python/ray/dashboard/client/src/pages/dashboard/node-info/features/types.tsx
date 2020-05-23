import React from "react";
import { NodeInfoResponse, RayletWorkerStats } from "../../../../api";

type ArrayType<T> = T extends Array<infer U> ? U : never;
export type Node = ArrayType<NodeInfoResponse["clients"]>;
export type Worker = ArrayType<Node["workers"]>;

type ClusterFeatureData = { nodes: Node[] };
type NodeFeatureData = { node: Node };
type WorkerFeatureData = {
  node: Node;
  worker: Worker;
  rayletWorker: RayletWorkerStats | null;
};

export type ClusterFeatureComponent = (
  data: ClusterFeatureData,
) => React.ReactElement;
export type NodeFeatureComponent = (
  data: NodeFeatureData,
) => React.ReactElement;
export type WorkerFeatureComponent = (
  data: WorkerFeatureData,
) => React.ReactElement;
