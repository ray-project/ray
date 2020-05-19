import React from "react";
import UsageBar from "../../../../common/UsageBar";
import {
  ClusterFeatureComponent,
  NodeFeatureComponent,
  WorkerFeatureComponent,
  Node,
} from "./types";
import { getWeightedAverage } from "../../../../common/util";

const clusterUtilization = (nodes: Array<Node>) => {
    return getWeightedAverage(
        nodes.map(node => ({ 
            weight: node.gpus.length, 
            value: nodeUtilization(node)})))
};

const nodeUtilization = (node: Node) => {
    const utilizationSum = node.gpus.reduce((acc, gpu) => acc + gpu.load, 0);
    const avgUtilization = utilizationSum / node.gpus.length;
    return avgUtilization;
}


export const ClusterGPU: ClusterFeatureComponent = ({ nodes }) => {
    const clusterAverageUtilization = clusterUtilization(nodes)
    return (
        <div style={{ minWidth: 60 }}>
        <UsageBar
            percent={clusterAverageUtilization}
            text={`${clusterAverageUtilization.toFixed(1)}%`}
        />
        </div>
    );
};

export const NodeGPU: NodeFeatureComponent = ({ node }) => (
  <div style={{ minWidth: 60 }}>
    <UsageBar percent={nodeUtilization(node)} text={`${node.cpu.toFixed(1)}%`} />
  </div>
);

export const WorkerGPU: WorkerFeatureComponent = ({ worker }) => (
  <div style={{ minWidth: 60 }}>
    <UsageBar
      percent={worker.cpu_percent}
      text={`${worker.cpu_percent.toFixed(1)}%`}
    />
  </div>
);