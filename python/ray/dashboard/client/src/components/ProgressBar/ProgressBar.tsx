import { Box, TooltipProps, Typography } from "@mui/material";
import React from "react";
import { RiArrowDownSLine, RiArrowRightSLine } from "react-icons/ri";
import { HelpInfo, StyledTooltip } from "../Tooltip";

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
    <Box sx={{ display: "flex", flexDirection: "column" }}>
      {(showLegend || controls) && (
        <Box
          display="flex"
          justifyContent="space-between"
          alignItems="center"
          marginBottom={1}
        >
          {showLegend && (
            <Box sx={{ display: "flex", flexDirection: "row" }}>
              <Box
                sx={{
                  display: "flex",
                  flexDirection: "row",
                  flexWrap: "nowrap",
                  alignItems: "center",
                  "&:not(:first-child)": {
                    marginLeft: 1.5,
                  },
                  "&:not(:last-child)": {
                    marginRight: 1.5,
                  },
                }}
              >
                <Box
                  sx={{
                    width: 16,
                    height: 16,
                    borderRadius: "4px",
                    marginRight: 1,
                    backgroundColor: "black",
                  }}
                />
                <Typography>Total: {finalTotal}</Typography>
              </Box>
              {filteredSegments.map(({ value, label, hint, color }) => (
                <Box
                  key={label}
                  sx={{
                    display: "flex",
                    flexDirection: "row",
                    flexWrap: "nowrap",
                    alignItems: "center",
                    "&:not(:first-child)": {
                      marginLeft: 1.5,
                    },
                    "&:not(:last-child)": {
                      marginRight: 1.5,
                    },
                  }}
                >
                  <Box
                    sx={{
                      width: 16,
                      height: 16,
                      borderRadius: "4px",
                      marginRight: 1,
                      backgroundColor: color,
                    }}
                  />
                  <Typography>
                    {label}: {value}
                  </Typography>
                  {hint && <HelpInfo sx={{ marginLeft: 0.5 }}>{hint}</HelpInfo>}
                </Box>
              ))}
            </Box>
          )}
          {controls && controls}
        </Box>
      )}
      <Box
        sx={{ display: "flex", flexDirection: "row", alignItems: "center" }}
        onClick={onClick}
      >
        {expanded !== undefined &&
          (expanded ? (
            <Box
              component={RiArrowDownSLine}
              sx={{
                width: 16,
                height: 16,
                marginRight: 1,
              }}
            />
          ) : (
            <Box
              component={RiArrowRightSLine}
              sx={{
                width: 16,
                height: 16,
                marginRight: 1,
              }}
            />
          ))}
        <LegendTooltip
          showTooltip={showTooltip}
          total={finalTotal}
          segments={filteredSegments}
        >
          <Box
            sx={{
              display: "flex",
              flexDirection: "row",
              flexWrap: "nowrap",
              width: "100%",
              height: 8,
              borderRadius: "6px",
              overflow: "hidden",
              backgroundColor: segmentTotal === 0 ? "lightGrey" : "white",
            }}
          >
            {filteredSegments.map(({ color, label, value }) => (
              <Box
                component="span"
                key={label}
                sx={{
                  "&:not(:last-child)": {
                    marginRight: "1px",
                  },
                  flex: value,
                  backgroundColor: color,
                }}
                data-testid="progress-bar-segment"
              />
            ))}
          </Box>
        </LegendTooltip>
        {showTotalProgress !== undefined && (
          <Box
            sx={{
              flex: "1 0 40px",
              marginLeft: 1,
              textAlign: "end",
              whiteSpace: "nowrap",
            }}
          >
            {showTotalProgress} / {finalTotal}
          </Box>
        )}
      </Box>
    </Box>
  );
};

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
  if (showTooltip) {
    return (
      <StyledTooltip
        placement="right"
        title={
          <Box>
            <Box
              sx={{
                display: "flex",
                flexDirection: "row",
                flexWrap: "nowrap",
                alignItems: "center",
                "&:not(:first-child)": {
                  marginTop: 1,
                },
              }}
            >
              <Box
                sx={{
                  width: 16,
                  height: 16,
                  borderRadius: "4px",
                  marginRight: 1,
                  backgroundColor: "black",
                }}
              />
              <Typography>Total: {total}</Typography>
            </Box>
            {segments.map(({ value, label, color }) => (
              <Box
                key={label}
                sx={{
                  display: "flex",
                  flexDirection: "row",
                  flexWrap: "nowrap",
                  alignItems: "center",
                  "&:not(:first-child)": {
                    marginTop: 1,
                  },
                }}
              >
                <Box
                  sx={{
                    width: 16,
                    height: 16,
                    borderRadius: "4px",
                    marginRight: 1,
                    backgroundColor: color,
                  }}
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
