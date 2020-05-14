import React from "react";
import UsageBar from "../../../../common/UsageBar";
import {
  ClusterFeatureComponent,
  NodeFeatureComponent,
  WorkerFeatureComponent,
} from "./types";

const getWeightedAverage = (
  input: {
    weight: number;
    value: number;
  }[],
) => {
  if (input.length === 0) {
    return 0;
  }

  let totalWeightTimesValue = 0;
  let totalWeight = 0;
  for (const { weight, value } of input) {
    totalWeightTimesValue += weight * value;
    totalWeight += weight;
  }
  return totalWeightTimesValue / totalWeight;
};

const clusterUtilization = (nodes) => {
    return getWeightedAverage(
        nodes.map(node => ({ 
            weight: node.gpus.length, 
            value: nodeUtilization(node)}));
};

const nodeUtilization = (node) => {
    const utilizationSum = node.gpus.reduce((acc, gpu) => acc + gpu.load, 0);
    const avgUtilization = utilizationSum / node.gpus.length;
    return node.gpus.length;
}

export const ClusterGPU: ClusterFeatureComponent = ({ nodes }) => {
    const clusterAverageUtilization = clusterUtilization(nodes)
  return (
    <div style={{ minWidth: 60 }}>
      <UsageBar
        percent={cpuWeightedAverage}
        text={`${cpuWeightedAverage.toFixed(1)}%`}
      />
    </div>
  );
};

export const NodeGPU: NodeFeatureComponent = ({ node }) => (
  <div style={{ minWidth: 60 }}>
    <UsageBar percent={node.cpu} text={`${node.cpu.toFixed(1)}%`} />
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

export const ClusterGRAM: ClusterFeatureComponent = ({ nodes }) => {
    const cpuWeightedAverage = getWeightedAverage(
      nodes.map((node) => ({ weight: node.cpus[0], value: node.cpu })),
    );
    return (
      <div style={{ minWidth: 60 }}>
        <UsageBar
          percent={cpuWeightedAverage}
          text={`${cpuWeightedAverage.toFixed(1)}%`}
        />
      </div>
    );
  };
  
  export const NodeGRAM: NodeFeatureComponent = ({ node }) => (
    <div style={{ minWidth: 60 }}>
      <UsageBar percent={node.cpu} text={`${node.cpu.toFixed(1)}%`} />
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