import React from "react";
import { formatByteAmount, formatUsage } from "../../../common/formatUtils";
import UsageBar from "../../../common/UsageBar";
import { NodeFeatureComponent, WorkerFeatureComponent } from "./types";

export const NodeRAM: NodeFeatureComponent = ({ node }) => (
  <UsageBar
    percent={(100 * (node.mem[0] - node.mem[1])) / node.mem[0]}
    text={formatUsage(node.mem[0] - node.mem[1], node.mem[0], "gibibyte")}
  />
);

export const WorkerRAM: WorkerFeatureComponent = ({ node, worker }) => (
  <UsageBar
    percent={(100 * worker.memory_info.rss) / node.mem[0]}
    text={formatByteAmount(worker.memory_info.rss, "mebibyte")}
  />
);
