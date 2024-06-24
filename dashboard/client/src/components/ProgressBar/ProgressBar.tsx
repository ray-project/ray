import { Box, Theme, TooltipProps, Typography, useTheme } from "@mui/material";
import React from "react";
import { RiArrowDownSLine, RiArrowRightSLine } from "react-icons/ri";
import { HelpInfo, StyledTooltip } from "../Tooltip";

const useStyles = (theme: Theme) => ({
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
    borderRadius: "4px",
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
    borderRadius: "6px",
    overflow: "hidden",
  },
  segment: {
    "&:not(:last-child)": {
      marginRight: "1px",
    },
  },
  progressTotal: {
    flex: "1 0 40px",
    marginLeft: theme.spacing(1),
    textAlign: "end",
    whiteSpace: "nowrap",
  },
});

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
  const styles = useStyles(useTheme());
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
            hint: "Unaccounted tasks can happen when there are too many tasks. Ray drops older tasks to conserve memory.",
            color: "#EEEEEE",
          },
        ]
      : progress;

  const filteredSegments = segments.filter(({ value }) => value);

  return (
    <Box sx={styles.root}>
      {(showLegend || controls) && (
        <Box
          display="flex"
          justifyContent="space-between"
          alignItems="center"
          marginBottom={1}
        >
          {showLegend && (
            <Box sx={styles.legendRoot}>
              <Box sx={styles.legendItemContainer}>
                <Box
                  sx={styles.colorLegend}
                  style={{ backgroundColor: "black" }}
                />
                <Typography>Total: {finalTotal}</Typography>
              </Box>
              {filteredSegments.map(({ value, label, hint, color }) => (
                <Box key={label} sx={styles.legendItemContainer}>
                  <Box
                    sx={styles.colorLegend}
                    style={{ backgroundColor: color }}
                  />
                  <Typography>
                    {label}: {value}
                  </Typography>
                  {hint && <HelpInfo sx={styles.hint}>{hint}</HelpInfo>}
                </Box>
              ))}
            </Box>
          )}
          {controls && controls}
        </Box>
      )}
      <Box sx={styles.progressBarContainer} onClick={onClick}>
        {expanded !== undefined &&
          (expanded ? (
            <Box component={RiArrowDownSLine} sx={styles.icon} />
          ) : (
            <Box component={RiArrowRightSLine} sx={styles.icon} />
          ))}
        <LegendTooltip
          showTooltip={showTooltip}
          total={finalTotal}
          segments={filteredSegments}
        >
          <Box
            sx={styles.progressBarRoot}
            style={{
              backgroundColor: segmentTotal === 0 ? "lightGrey" : "white",
            }}
          >
            {filteredSegments.map(({ color, label, value }) => (
              <Box
                component="span"
                key={label}
                sx={styles.segment}
                style={{
                  flex: value,
                  backgroundColor: color,
                }}
                data-testid="progress-bar-segment"
              />
            ))}
          </Box>
        </LegendTooltip>
        {showTotalProgress !== undefined && (
          <Box sx={styles.progressTotal}>
            {showTotalProgress} / {finalTotal}
          </Box>
        )}
      </Box>
    </Box>
  );
};

const useLegendStyles = (theme: Theme) => ({
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
    borderRadius: "4px",
    marginRight: theme.spacing(1),
  },
});

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
  const styles = useLegendStyles(useTheme());

  if (showTooltip) {
    return (
      <StyledTooltip
        placement="right"
        title={
          <Box>
            <Box sx={styles.legendItemContainer}>
              <Box
                sx={styles.colorLegend}
                style={{ backgroundColor: "black" }}
              />
              <Typography>Total: {total}</Typography>
            </Box>
            {segments.map(({ value, label, color }) => (
              <Box key={label} sx={styles.legendItemContainer}>
                <Box
                  sx={styles.colorLegend}
                  style={{ backgroundColor: color }}
                />
                <Typography>
                  {label}: {value}
                </Typography>
              </Box>
            ))}
          </Box>
        }
      >
        {children}
      </StyledTooltip>
    );
  }

  return children;
};
