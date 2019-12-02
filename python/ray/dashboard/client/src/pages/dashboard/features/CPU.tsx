import React from "react";
import UsageBar from "../../../common/UsageBar";
import { NodeFeatureComponent, WorkerFeatureComponent } from "./types";

export const NodeCPU: NodeFeatureComponent = ({ node }) => (
  <div style={{ minWidth: 60 }}>
    <UsageBar percent={node.cpu} text={`${node.cpu.toFixed(1)}%`} />
  </div>
);

export const WorkerCPU: WorkerFeatureComponent = ({ worker }) => (
  <div style={{ minWidth: 60 }}>
    <UsageBar
      percent={worker.cpu_percent}
      text={`${worker.cpu_percent.toFixed(1)}%`}
    />
  </div>
);
