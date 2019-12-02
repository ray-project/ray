import React from "react";
import { formatUptime } from "../../../common/formatUtils";
import { NodeFeatureComponent, WorkerFeatureComponent } from "./types";

export const NodeUptime: NodeFeatureComponent = ({ node }) => (
  <React.Fragment>{formatUptime(node.boot_time)}</React.Fragment>
);

export const WorkerUptime: WorkerFeatureComponent = ({ worker }) => (
  <React.Fragment>{formatUptime(worker.create_time)}</React.Fragment>
);
