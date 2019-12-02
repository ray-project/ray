import React from "react";
import { formatUptime } from "../../../common/formatUtils";
import { makeFeature } from "./Feature";

const Uptime = makeFeature({
  getFeatureForNode: ({ node }) => (
    <React.Fragment>{formatUptime(node.boot_time)}</React.Fragment>
  ),
  getFeatureForWorker: ({ worker }) => (
    <React.Fragment>{formatUptime(worker.create_time)}</React.Fragment>
  )
});

export default Uptime;
