import React from "react";
import { makeFeature } from "./Feature";

const Host = makeFeature({
  getFeatureForNode: ({ node }) => (
    <React.Fragment>
      {node.hostname} ({node.ip})
    </React.Fragment>
  ),
  getFeatureForWorker: ({ worker }) => (
    <React.Fragment>
      {worker.cmdline[0].split("::", 2)[0]} (PID: {worker.pid})
    </React.Fragment>
  )
});

export default Host;
