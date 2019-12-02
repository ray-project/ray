import React from "react";
import { NodeInfoResponse } from "../../../api";

type ArrayType<T> = T extends Array<infer U> ? U : never;
type Node = ArrayType<NodeInfoResponse["clients"]>;
type Worker = ArrayType<Node["workers"]>;

interface Props {
  getFeatureForNode({ node }: { node: Node }): React.ReactElement;
  getFeatureForWorker({
    node,
    worker
  }: {
    node: Node;
    worker: Worker;
  }): React.ReactElement;
  data:
    | { type: "node"; node: Node }
    | { type: "worker"; node: Node; worker: Worker };
}

class Feature extends React.Component<Props> {
  render() {
    const { getFeatureForNode, getFeatureForWorker, data } = this.props;
    return data.type === "node"
      ? getFeatureForNode(data)
      : getFeatureForWorker(data);
  }
}

export const makeFeature = (
  partialProps: Pick<Props, "getFeatureForNode" | "getFeatureForWorker">
): React.FunctionComponent<Props["data"]> => (data: Props["data"]) => (
  <Feature
    getFeatureForNode={partialProps.getFeatureForNode}
    getFeatureForWorker={partialProps.getFeatureForWorker}
    data={data}
  />
);
