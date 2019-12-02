import React from "react";
import UsageBar from "../../../common/UsageBar";
import { makeFeature } from "./makeFeature";

const CPU = makeFeature({
  getFeatureForNode: ({ node }) => (
    <div style={{ minWidth: 60 }}>
      <UsageBar percent={node.cpu} text={`${node.cpu.toFixed(1)}%`} />
    </div>
  ),
  getFeatureForWorker: ({ worker }) => (
    <div style={{ minWidth: 60 }}>
      <UsageBar
        percent={worker.cpu_percent}
        text={`${worker.cpu_percent.toFixed(1)}%`}
      />
    </div>
  )
});

export default CPU;
