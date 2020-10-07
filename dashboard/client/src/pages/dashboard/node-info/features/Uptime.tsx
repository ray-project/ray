import { Typography } from "@material-ui/core";
import React from "react";
import { formatDuration } from "../../../../common/formatUtils";
import { Accessor } from "../../../../common/tableUtils";
import {
  ClusterFeatureRenderFn,
  NodeFeatureData,
  NodeFeatureRenderFn,
  NodeInfoFeature,
  WorkerFeatureData,
  WorkerFeatureRenderFn,
} from "./types";

const getUptime = (bootTime: number) => Date.now() / 1000 - bootTime;

export const ClusterUptime: ClusterFeatureRenderFn = ({ nodes }) => (
  <Typography color="textSecondary" component="span" variant="inherit">
    N/A
  </Typography>
);

export const NodeUptime: NodeFeatureRenderFn = ({ node }) => (
  <React.Fragment>{formatDuration(getUptime(node.bootTime))}</React.Fragment>
);

export const nodeUptimeAccessor: Accessor<NodeFeatureData> = ({ node }) =>
  getUptime(node.bootTime);

export const WorkerUptime: WorkerFeatureRenderFn = ({ worker }) => (
  <React.Fragment>
    {formatDuration(getUptime(worker.createTime))}
  </React.Fragment>
);

const workerUptimeAccessor: Accessor<WorkerFeatureData> = ({ worker }) =>
  getUptime(worker.createTime);

const uptimeFeature: NodeInfoFeature = {
  id: "uptime",
  NodeFeatureRenderFn: NodeUptime,
  WorkerFeatureRenderFn: WorkerUptime,
  nodeAccessor: nodeUptimeAccessor,
  workerAccessor: workerUptimeAccessor,
};

export default uptimeFeature;
