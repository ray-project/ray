import {
  createStyles,
  makeStyles,
  Paper,
  TooltipProps,
  Typography,
} from "@material-ui/core";
import React from "react";
import { StyledTooltip } from "../Tooltip";

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
      display: "flex",
      flexDirection: "row",
      flexWrap: "nowrap",
      width: "100%",
      height: 8,
      backgroundColor: "white",
      borderRadius: 6,
      overflow: "hidden",
    },
    segment: {
      "&:not(:last-child)": {
        marginRight: 1,
      },
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
  /**
   * Whether a legend is shown. Default to true.
   */
  showLegend?: boolean;
  /**
   * Whether to show the a legend as a tooltip.
   */
  showTooltip?: boolean;
};

export const ProgressBar = ({
  progress,
  total,
  unaccountedLabel,
  showLegend = true,
  showTooltip = false,
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

  const filteredSegments = segments.filter(({ value }) => value);

  return (
    <div className={classes.root}>
      {showLegend && (
        <div className={classes.legendRoot}>
          <div className={classes.legendItemContainer}>
            <div
              className={classes.colorLegend}
              style={{ backgroundColor: "black" }}
            />
            <Typography>Total: {finalTotal}</Typography>
          </div>
          {filteredSegments.map(({ value, label, color }) => (
            <div key={label} className={classes.legendItemContainer}>
              <div
                className={classes.colorLegend}
                style={{ backgroundColor: color }}
              />
              <Typography>
                {label}: {value}
              </Typography>
            </div>
          ))}
        </div>
      )}
      <LegendTooltip
        showTooltip={showTooltip}
        total={finalTotal}
        segments={filteredSegments}
      >
        <div
          className={classes.progressBarRoot}
          style={{
            backgroundColor: segmentTotal === 0 ? "lightGrey" : "white",
          }}
        >
          {filteredSegments.map(({ color, label, value }) => (
            <span
              key={label}
              className={classes.segment}
              style={{
                flex: value,
                backgroundColor: color,
              }}
              data-testid="progress-bar-segment"
            />
          ))}
        </div>
      </LegendTooltip>
    </div>
  );
};

const useLegendStyles = makeStyles((theme) =>
  createStyles({
    legendItemContainer: {
      display: "flex",
      flexDirection: "row",
      flexWrap: "nowrap",
      alignItems: "center",
      "&:not(:first-child)": {
        marginTop: theme.spacing(1),
      },
    },
    colorLegend: {
      width: 16,
      height: 16,
      borderRadius: 4,
      marginRight: theme.spacing(1),
    },
  }),
);

type LegendTooltipProps = {
  showTooltip: boolean;
  segments: ProgressBarSegment[];
  total: number;
  children: TooltipProps["children"];
};

const LegendTooltip = ({
  showTooltip,
  segments,
  total,
  children,
}: LegendTooltipProps) => {
  const classes = useLegendStyles();

  if (showTooltip) {
    return (
      <StyledTooltip
        placement="right"
        title={
          <Paper>
            <div className={classes.legendItemContainer}>
              <div
                className={classes.colorLegend}
                style={{ backgroundColor: "black" }}
              />
              <Typography>Total: {total}</Typography>
            </div>
            {segments.map(({ value, label, color }) => (
              <div key={label} className={classes.legendItemContainer}>
                <div
                  className={classes.colorLegend}
                  style={{ backgroundColor: color }}
                />
                <Typography>
                  {label}: {value}
                </Typography>
              </div>
            ))}
          </Paper>
        }
      >
        {children}
      </StyledTooltip>
    );
  }

  return children;
};
