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
    ["Total Local Reference Count", `${memoryTableSummary.totalLocalRefCount}`],
    ["Pinned in Memory Count", `${memoryTableSummary.totalPinnedInMemory}`],
    [
      "Total Used by Pending Tasks Count",
      `${memoryTableSummary.totalUsedByPendingTask}`,
    ],
    [
      "Total Captured in Objects Count",
      `${memoryTableSummary.totalCapturedInObjects}`,
    ],
    [
      "Total Memory Used by Objects",
      `${formatByteAmount(memoryTableSummary.totalObjectSize, "mebibyte")}`,
    ],
    ["Total Actor Handle Count", `${memoryTableSummary.totalActorHandles}`],
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
