import { createStyles, Grid, makeStyles, Theme } from "@material-ui/core";
import React from "react";
import { MemoryTableSummary } from "../../../api";
import { formatByteAmount } from "../../../common/formatUtils";
import LabeledDatum from "../../../common/LabeledDatum";

const useMemorySummaryStyles = makeStyles((theme: Theme) =>
  createStyles({
    expandCollapseIcon: {
      color: theme.palette.text.secondary,
      fontSize: "1.5em",
      verticalAlign: "middle",
    },
    container: {
      padding: theme.spacing(1),
      margin: theme.spacing(1),
    },
  }),
);

type MemorySummaryProps = {
  memoryTableSummary: MemoryTableSummary;
  initialExpanded: boolean;
};

const MemorySummary: React.FC<MemorySummaryProps> = ({
  memoryTableSummary,
}) => {
  const classes = useMemorySummaryStyles();
  const memoryData = [
    [
      "Total Local Reference Count",
      `${memoryTableSummary.total_local_ref_count}`,
    ],
    ["Pinned in Memory Count", `${memoryTableSummary.total_pinned_in_memory}`],
    [
      "Total Used by Pending Tasks Count",
      `${memoryTableSummary.total_used_by_pending_task}`,
    ],
    [
      "Total Captured in Objects Count",
      `${memoryTableSummary.total_captured_in_objects}`,
    ],
    [
      "Total Memory Used by Objects",
      `${formatByteAmount(memoryTableSummary.total_object_size, "mebibyte")}`,
    ],
    ["Total Actor Handle Count", `${memoryTableSummary.total_actor_handles}`],
  ];

  return (
    <Grid container className={classes.container}>
      {memoryData.map(([label, value]) => (
        <LabeledDatum key={label} label={label} datum={value} />
      ))}
    </Grid>
  );
};
export default MemorySummary;
