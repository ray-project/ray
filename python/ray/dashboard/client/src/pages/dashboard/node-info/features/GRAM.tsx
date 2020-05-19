import { ClusterFeatureComponent, NodeFeatureComponent, WorkerFeatureComponent, Node, Worker } from "./types";
import React from "react";
import UsageBar from "../../../../common/UsageBar";
import { getWeightedAverage } from "../../../../common/util";

// TODO SUPPORT N/A instead of 0 
const nodeGRAMUtilization = (node: Node) => {
    const utilizationSum = node.gpus.reduce((acc, gpu) => acc + gpu.memory_util, 0);
    const avgUtilization = utilizationSum / node.gpus.length;
    return avgUtilization;
};

const workerGRAMUtilization = (worker: Worker) => {
    return undefined
};

const clusterGRAMUtilization = (nodes: Array<Node>) => {
    return getWeightedAverage(
        nodes.map(node => ({
            weight: node.gpus.length,
            value: nodeGRAMUtilization(node)
        }))
    )
};

export const ClusterGRAM: ClusterFeatureComponent = ({ nodes }) => {
    const clusterAverageUtilization = clusterGRAMUtilization(nodes)
    return (
        <div style={{ minWidth: 60 }}>
        <UsageBar
          percent={clusterAverageUtilization}
          text={`${clusterAverageUtilization.toFixed(1)}%`}
        />
        </div>
    );
  };

  // TODO FIGURE OUT WHETHER I CAN GET THE PID USING THE GPU FROM THE 
  // INFO IN NVIDIA SMI OR WHETHER I NEED TO DO A MERGE WITH ACTOR.
  
  export const NodeGRAM: NodeFeatureComponent = ({ node }) => (
    <div style={{ minWidth: 60 }}>
      <UsageBar percent={nodeGRAMUtilization(node)} text={`${node.cpu.toFixed(1)}%`} />
    </div>
  );
  
  export const WorkerGRAM: WorkerFeatureComponent = ({ worker }) => (
    <div style={{ minWidth: 60 }}>
      <UsageBar
        percent={worker.cpu_percent}
        text={`${worker.cpu_percent.toFixed(1)}%`}
      />
    </div>
  );