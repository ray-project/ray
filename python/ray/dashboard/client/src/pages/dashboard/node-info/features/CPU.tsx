import React from "react";
import UsageBar from "../../../../common/UsageBar";
import { Accessor } from "../../../../common/tableUtils";
import { getWeightedAverage } from "../../../../common/util";
import {
  ClusterFeatureRenderFn,
  NodeFeatureRenderFn,
  WorkerFeatureComponent,
  ClusterFeatureData,
  NodeFeatureData,
  WorkerFeatureData
} from "./types";

export const ClusterCPU: ClusterFeatureRenderFn = ({ nodes }) => {
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

export const NodeCPU: NodeFeatureRenderFn = ({ node }) => (
  <div style={{ minWidth: 60 }}>
    <UsageBar percent={node.cpu} text={`${node.cpu.toFixed(1)}%`} />
  </div>
);
export const NodeCPUComparator: Accessor<NodeFeatureData> = ({ node }) => {
  return node.cpu;
}

export const WorkerCPU: WorkerFeatureComponent = ({ worker }) => (
  <div style={{ minWidth: 60 }}>
    <UsageBar
      percent={worker.cpu_percent}
      text={`${worker.cpu_percent.toFixed(1)}%`}
    />
  </div>
);

export const WorkerCPUComparator: Accessor<WorkerFeatureData> = ({ worker }) => {
  return worker.cpu_percent;
}
