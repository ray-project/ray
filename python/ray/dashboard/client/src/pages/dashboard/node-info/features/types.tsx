import React from "react";
import { NodeInfoResponse, RayletWorkerStats } from "../../../../api";
import { Accessor } from "../../../../common/tableUtils"

type ArrayType<T> = T extends Array<infer U> ? U : never;
export type Node = ArrayType<NodeInfoResponse["clients"]>;
export type Worker = ArrayType<Node["workers"]>;

type ClusterFeatureData = { nodes: Node[] };
export type NodeFeatureData = { node: Node };
export type WorkerFeatureData = {
  node: Node;
  worker: Worker;
  rayletWorker: RayletWorkerStats | null;
};

export type NodeAggregations = {
  [ip: string]: NodeAggregation;
};

export type NodeAggregation = {
  perWorker: {
    [pid: string]: number;
  };
  total: number;
};

export type ClusterFeatureRenderFn = (
  data: ClusterFeatureData,
) => React.ReactElement;
export type NodeFeatureRenderFn = (
  data: NodeFeatureData,
) => React.ReactElement;
export type WorkerFeatureRenderFn = (
  data: WorkerFeatureData,
) => React.ReactElement;

export type NodeInfoFeature = {
  WorkerFeatureRenderFn: WorkerFeatureRenderFn,
  NodeFeatureRenderFn: NodeFeatureRenderFn,
  ClusterFeatureRenderFn?: ClusterFeatureRenderFn,
  workerAccessor?: Accessor<WorkerFeatureData>,
  nodeAccessor?: Accessor<NodeFeatureData>
}
