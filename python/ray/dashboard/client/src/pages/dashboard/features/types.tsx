import React from "react";
import { NodeInfoResponse } from "../../../api";

type ArrayType<T> = T extends Array<infer U> ? U : never;
type Node = ArrayType<NodeInfoResponse["clients"]>;
type Worker = ArrayType<Node["workers"]>;

type NodeFeatureData = { node: Node };
type WorkerFeatureData = { node: Node; worker: Worker };

export type NodeFeatureComponent = (
  data: NodeFeatureData
) => React.ReactElement;
export type WorkerFeatureComponent = (
  data: WorkerFeatureData
) => React.ReactElement;
