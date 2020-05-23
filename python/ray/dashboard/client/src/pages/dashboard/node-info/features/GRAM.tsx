import { ClusterFeatureComponent, NodeFeatureComponent, WorkerFeatureComponent, Node, Worker } from "./types";
import React from "react";
import UsageBar from "../../../../common/UsageBar";
import { GPUStats } from "../../../../api";
import { getWeightedAverage } from "../../../../common/util";
import { MiBRatio } from "../../../../common/formatUtils";

const nodeGRAMUtilization = (node: Node) => {
    const utilization =  (gpu: GPUStats) => gpu.memory_used / gpu.memory_total; 
    const utilizationSum = node.gpus.reduce((acc, gpu) => acc + utilization(gpu), 0);
    const avgUtilization = utilizationSum / node.gpus.length;
    // Convert to a percent before returning
    return avgUtilization * 100;
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
    const workerProcessPerGPU = node.gpus.map(
      gpu => gpu.processes).map(processes => processes.find(process => process.pid === worker.pid));
    const workerUtilPerGPU = workerProcessPerGPU.map(proc => proc?.gpu_memory_usage || 0);
    const totalGRAMperGPU = node.gpus.map(gpu => gpu.memory_total);
    const totalNodeGRAM = totalGRAMperGPU.reduce((acc, usage) => acc + usage, 0);
    const usedGRAM = workerUtilPerGPU.reduce((acc, usage) => acc + usage, 0);
    return (
    <div style={{ minWidth: 60 }}>
      <UsageBar
        percent={100 * (usedGRAM / totalNodeGRAM)}       
        text={MiBRatio(usedGRAM, totalNodeGRAM)}
      />
    </div>
  )};