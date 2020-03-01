import React from "react";
import { formatByteAmount, formatUsage } from "../../../../common/formatUtils";
import UsageBar from "../../../../common/UsageBar";
import {
  ClusterFeatureComponent,
  NodeFeatureComponent,
  WorkerFeatureComponent
} from "./types";
import SpanButton from "../../../../common/SpanButton";

export const ClusterRAM: ClusterFeatureComponent = ({ nodes }) => {
  let used = 0;
  let total = 0;
  for (const node of nodes) {
    used += node.mem[0] - node.mem[1];
    total += node.mem[0];
  }
  return (
    <UsageBar
      percent={(100 * used) / total}
      text={formatUsage(used, total, "gibibyte")}
    />
  );
};

type setIframeDialogType = (
  pid: number | "All",
  metric: "cpu" | "memory"
) => void;

export const makeNodeRAM = (
  setIframeDialog: setIframeDialogType
): NodeFeatureComponent => ({ node }) => (
  <SpanButton onClick={() => setIframeDialog("All", "memory")}>
    <UsageBar
      percent={(100 * (node.mem[0] - node.mem[1])) / node.mem[0]}
      text={formatUsage(node.mem[0] - node.mem[1], node.mem[0], "gibibyte")}
    />
  </SpanButton>
);

export const makeWorkerRAM = (
  setIframeDialog: setIframeDialogType
): WorkerFeatureComponent => ({ node, worker }) => (
  <SpanButton onClick={() => setIframeDialog(worker.pid, "memory")}>
    <UsageBar
      percent={(100 * worker.memory_info.rss) / node.mem[0]}
      text={formatByteAmount(worker.memory_info.rss, "mebibyte")}
    />
  </SpanButton>
);
