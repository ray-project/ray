import React from "react";
import { makeFeature } from "./makeFeature";

const Workers = makeFeature({
  getFeatureForNode: ({ node }) => (
    <React.Fragment>{node.workers.length}</React.Fragment>
  ),
  getFeatureForWorker: ({ worker }) => (
    <React.Fragment>{worker.cmdline[0].split("::", 2)[1]}</React.Fragment>
  )
});

export default Workers;
