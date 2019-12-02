import React from "react";
import { NodeInfoResponse } from "../../../api";

type ArrayType<T> = T extends Array<infer U> ? U : never;
type Node = ArrayType<NodeInfoResponse["clients"]>;
type Worker = ArrayType<Node["workers"]>;

export const makeFeature = ({
  getFeatureForNode,
  getFeatureForWorker
}: {
  getFeatureForNode: ({ node }: { node: Node }) => React.ReactElement;
  getFeatureForWorker: ({
    node,
    worker
  }: {
    node: Node;
    worker: Worker;
  }) => React.ReactElement;
}) => (
  data:
    | { type: "node"; node: Node }
    | { type: "worker"; node: Node; worker: Worker }
) =>
  data.type === "node" ? getFeatureForNode(data) : getFeatureForWorker(data);
