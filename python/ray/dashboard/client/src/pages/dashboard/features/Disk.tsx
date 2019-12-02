import React from "react";
import { formatUsage } from "../../../common/formatUtils";
import UsageBar from "../../../common/UsageBar";
import { makeFeature } from "./makeFeature";
import Typography from "@material-ui/core/Typography";

const Disk = makeFeature({
  getFeatureForNode: ({ node }) => (
    <React.Fragment>
      <UsageBar
        percent={(100 * node.disk["/"].used) / node.disk["/"].total}
        text={formatUsage(
          node.disk["/"].used,
          node.disk["/"].total,
          "gibibyte"
        )}
      />
    </React.Fragment>
  ),
  getFeatureForWorker: () => (
    <Typography color="textSecondary" component="span" variant="inherit">
      Not available
    </Typography>
  )
});

export default Disk;
