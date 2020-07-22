import React from "react";
import { Accessor } from "../../../../common/tableUtils";
import UsageBar from "../../../../common/UsageBar";
import { getWeightedAverage } from "../../../../common/util";
import {
  ClusterFeature,
  NodeFeature,
  NodeFeatureData,
  NodeInfoFeature,
  WorkerFeature,
  WorkerFeatureData,
} from "./types";

export const ClusterCPU: ClusterFeature = ({ nodes }) => {
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

export const NodeCPU: NodeFeature = ({ node }) => (
  <div style={{ minWidth: 60 }}>
    <UsageBar percent={node.cpu} text={`${node.cpu.toFixed(1)}%`} />
  </div>
);
export const nodeCPUAccessor: Accessor<NodeFeatureData> = ({ node }) => {
  return node.cpu;
};

export const WorkerCPU: WorkerFeature = ({ worker }) => (
  <div style={{ minWidth: 60 }}>
    <UsageBar
      percent={worker.cpuPercent}
      text={`${worker.cpuPercent.toFixed(1)}%`}
    />
  </div>
);

export const workerCPUAccessor: Accessor<WorkerFeatureData> = ({ worker }) => {
  return worker.cpuPercent;
};

const cpuFeature: NodeInfoFeature = {
  id: "cpu",
  ClusterFeature: ClusterCPU,
  NodeFeature: NodeCPU,
  WorkerFeature: WorkerCPU,
  nodeAccessor: nodeCPUAccessor,
  workerAccessor: workerCPUAccessor,
};

export default cpuFeature;
