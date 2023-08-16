import {
  Box,
  createStyles,
  makeStyles,
  Paper,
  TooltipProps,
  Typography,
} from "@material-ui/core";
import React from "react";
import { RiArrowDownSLine, RiArrowRightSLine } from "react-icons/ri";
import { HelpInfo, StyledTooltip } from "../Tooltip";

const useStyles = makeStyles((theme) =>
  createStyles({
    root: {
      display: "flex",
      flexDirection: "column",
    },
    legendRoot: {
      display: "flex",
      flexDirection: "row",
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
    hint: {
      marginLeft: theme.spacing(0.5),
    },
    progressBarContainer: {
      display: "flex",
      flexDirection: "row",
      alignItems: "center",
    },
    icon: {
      width: 16,
      height: 16,
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
    progressTotal: {
      flex: "1 0 40px",
      marginLeft: theme.spacing(1),
      textAlign: "end",
      whiteSpace: "nowrap",
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
   * Text to show to explain the segment better.
   */
  hint?: string;
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
  /**
   * Whether to show the total progress to the right of the progress bar.
   * Example: 5 / 20
   * This should be set to the number that should be shown in the left side of the fraction.
   * If this is undefined, don't show it.
   */
  showTotalProgress?: number;
  /**
   * If true, we show an expanded icon to the left of the progress bar.
   * If false, we show an unexpanded icon to the left of the progress bar.
   * If undefined, we don't show any icon.
   */
  expanded?: boolean;
  onClick?: () => void;
  /**
   * Controls that can be put to the right of the legend.
   */
  controls?: JSX.Element;
};

export const ProgressBar = ({
  progress,
  total,
  unaccountedLabel,
  showLegend = true,
  showTooltip = false,
  showTotalProgress,
  expanded,
  onClick,
  controls,
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
            label: unaccountedLabel ?? "Unaccounted",
            hint:
              "Unaccounted tasks can happen when there are too many tasks for the Ray Dashboard " +
              "to keep track of. Ray Dashboard will garbage collect old task data to conserve memory.\n" +
              "NOTE: This is only an UI limitation. Ray Dashboard only garbage collects tasks that have " +
              "already finished or errored.",
            color: "#EEEEEE",
          },
        ]
      : progress;

  const filteredSegments = segments.filter(({ value }) => value);

  return (
    <div className={classes.root}>
      {(showLegend || controls) && (
        <Box
          display="flex"
          justifyContent="space-between"
          alignItems="center"
          marginBottom={1}
        >
          {showLegend && (
            <div className={classes.legendRoot}>
              <div className={classes.legendItemContainer}>
                <div
                  className={classes.colorLegend}
                  style={{ backgroundColor: "black" }}
                />
                <Typography>Total: {finalTotal}</Typography>
              </div>
              {filteredSegments.map(({ value, label, hint, color }) => (
                <div key={label} className={classes.legendItemContainer}>
                  <div
                    className={classes.colorLegend}
                    style={{ backgroundColor: color }}
                  />
                  <Typography>
                    {label}: {value}
                  </Typography>
                  {hint && <HelpInfo className={classes.hint}>{hint}</HelpInfo>}
                </div>
              ))}
            </div>
          )}
          {controls && controls}
        </Box>
      )}
      <div className={classes.progressBarContainer} onClick={onClick}>
        {expanded !== undefined &&
          (expanded ? (
            <RiArrowDownSLine className={classes.icon} />
          ) : (
            <RiArrowRightSLine className={classes.icon} />
          ))}
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
        {showTotalProgress !== undefined && (
          <div className={classes.progressTotal}>
            {showTotalProgress} / {finalTotal}
          </div>
        )}
      </div>
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
