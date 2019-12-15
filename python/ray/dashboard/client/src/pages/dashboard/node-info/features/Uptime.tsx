import React from "react";
import { formatDuration } from "../../../../common/formatUtils";
import {
  ClusterFeatureComponent,
  NodeFeatureComponent,
  WorkerFeatureComponent
} from "./types";

const getUptime = (bootTime: number) => Date.now() / 1000 - bootTime;

export const ClusterUptime: ClusterFeatureComponent = ({ nodes }) => {
  let totalUptime = 0;
  for (const node of nodes) {
    totalUptime += getUptime(node.boot_time);
  }
  return <React.Fragment>{formatDuration(totalUptime)}</React.Fragment>;
};

export const NodeUptime: NodeFeatureComponent = ({ node }) => (
  <React.Fragment>{formatDuration(getUptime(node.boot_time))}</React.Fragment>
);

export const WorkerUptime: WorkerFeatureComponent = ({ worker }) => (
  <React.Fragment>
    {formatDuration(getUptime(worker.create_time))}
  </React.Fragment>
);
