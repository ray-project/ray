import Typography from "@material-ui/core/Typography";
import React from "react";
import { formatUsage } from "../../../common/formatUtils";
import UsageBar from "../../../common/UsageBar";
import { NodeFeatureComponent, WorkerFeatureComponent } from "./types";

export const NodeDisk: NodeFeatureComponent = ({ node }) => (
  <React.Fragment>
    <UsageBar
      percent={(100 * node.disk["/"].used) / node.disk["/"].total}
      text={formatUsage(node.disk["/"].used, node.disk["/"].total, "gibibyte")}
    />
  </React.Fragment>
);

export const WorkerDisk: WorkerFeatureComponent = () => (
  <Typography color="textSecondary" component="span" variant="inherit">
    Not available
  </Typography>
);
