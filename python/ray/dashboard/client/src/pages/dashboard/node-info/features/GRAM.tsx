import { ClusterFeatureComponent, NodeFeatureComponent, WorkerFeatureComponent, Node, Worker } from "./types";
import React from "react";
import UsageBar from "../../../../common/UsageBar";
import { GPUStats, GPUProcessStats } from "../../../../api";
import { getWeightedAverage } from "../../../../common/util";

// TODO SUPPORT N/A instead of 0 
const nodeGRAMUtilization = (node: Node) => {
    const utilization =  (gpu: GPUStats) => gpu.memory_used / gpu.memory_total; 
    const utilizationSum = node.gpus.reduce((acc, gpu) => acc + utilization(gpu), 0);
    const avgUtilization = utilizationSum / node.gpus.length;
    return avgUtilization;
};

const workerGRAMUtilization = ( gpuProcess: GPUProcessStats | undefined) => {
  return gpuProcess?.gpu_memory_usage || 0;
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

  export const NodeGRAM: NodeFeatureComponent = ({ node }) => {
    const gramUtil = nodeGRAMUtilization(node);
    return (
    <div style={{ minWidth: 60 }}>
      <UsageBar percent={gramUtil} text={`${gramUtil.toFixed(1)}%`}/>
    </div>
  )};
  
  export const WorkerGRAM: WorkerFeatureComponent = ({ worker, node }) => {
    const gramUtil = workerGRAMUtilization(worker);
    return (
    <div style={{ minWidth: 60 }}>
      <UsageBar
        percent={gramUtil}
        text={`${gramUtil.toFixed(1)}%`}
      />
    </div>
  )};