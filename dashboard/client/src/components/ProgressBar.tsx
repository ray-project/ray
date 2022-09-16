import { createStyles, makeStyles } from "@material-ui/core";
import React from "react";

const useStyles = makeStyles((theme) =>
  createStyles({
    root: {
      display: "flex",
      flexDirection: "column",
    },
    legendRoot: {
      display: "flex",
      flexDirection: "row",
      marginBottom: theme.spacing(2),
    },
    legendItemContainer: {
      display: "flex",
      flexDirection: "row",
      flexWrap: "nowrap",
      alignItems: "center",
      "&:not(:first-child)": {
        marginLeft: theme.spacing(1.5),
      },
      "&:not(:last-child)": {
        marginRight: theme.spacing(1.5),
      },
    },
    colorLegend: {
      width: 16,
      height: 16,
      borderRadius: 4,
      marginRight: theme.spacing(1),
    },
    progressBarRoot: {
      display: "block",
      position: "relative",
      width: "100%",
      height: 8,
      backgroundColor: "lightgrey",
      borderRadius: 6,
      overflow: "hidden",
    },
    segment: {
      display: "block",
      position: "absolute",
      height: "100%",
      borderStartEndRadius: "6px",
      borderEndEndRadius: "6px",
    },
  }),
);

export type ProgressBarSegment = {
  /**
   * Number of items in this segment
   */
  value: number;
  /**
   * Name of this segment
   */
  label: string;
  /**
   * A CSS color used to represent the segment.
   */
  color: string;
};

export type ProgressBarProps = {
  /**
   * The different segments to the progress bar.
   * The order determines the order of which we show the segments on the page.
   * Ex: [Succeeded: 5, Running: 2, Pending: 10]
   */
  progress: ProgressBarSegment[];
  /**
   * The expected total number of items. If not provided, we calculate the total
   * from the sum of the segments.
   *
   * If the sum of the values from each segment is less than total, then we create
   * an additional segment for unaccounted items. This additional segment is placed
   * at the end.
   */
  total?: number;
  /**
   * Label for unaccounted for items i.e. items that are not part of a `progress` segment.
   */
  unaccountedLabel?: string;
};

export const ProgressBar = ({
  progress,
  total,
  unaccountedLabel,
}: ProgressBarProps) => {
  const classes = useStyles();
  const segmentTotal = progress.reduce((acc, { value }) => acc + value, 0);
  const finalTotal = total ?? segmentTotal;

  // TODO(aguo): Handle total being > segmentTotal
  const segments =
    segmentTotal < finalTotal
      ? [
          ...progress,
          {
            value: finalTotal - segmentTotal,
            label: unaccountedLabel ?? "unaccounted",
            color: "#EEEEEE",
          },
        ]
      : progress;

  const segmentsWithWidth: (ProgressBarSegment & { width: string })[] = [];
  let runningTotal = 0;
  segments.forEach((segment) => {
    const { value } = segment;
    segmentsWithWidth.push({
      ...segment,
      width: `${((value + runningTotal) / finalTotal) * 100}%`,
    });
    runningTotal += value;
  });
  // Reverse because default z-indexing is elements on the DOM will overlap elements before them
  // and we want to show earlier items above later items.
  segmentsWithWidth.reverse();

  return (
    <div className={classes.root}>
      <div className={classes.legendRoot}>
        <div className={classes.legendItemContainer}>
          <div
            className={classes.colorLegend}
            style={{ backgroundColor: "black" }}
          />
          Total: {finalTotal}
        </div>
        {segments.map(({ value, label, color }) => (
          <div key={label} className={classes.legendItemContainer}>
            <div
              className={classes.colorLegend}
              style={{ backgroundColor: color }}
            />
            {label}: {value}
          </div>
        ))}
      </div>
      <div className={classes.progressBarRoot}>
        {segmentsWithWidth.map(({ width, color, label }) => (
          <span
            key={label}
            className={classes.segment}
            style={{
              width,
              backgroundColor: color,
            }}
          />
        ))}
      </div>
    </div>
  );
};
