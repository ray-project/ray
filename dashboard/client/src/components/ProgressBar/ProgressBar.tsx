import { Box, TooltipProps, Typography } from "@mui/material";
import { styled } from "@mui/material/styles";
import React from "react";
import { RiArrowDownSLine, RiArrowRightSLine } from "react-icons/ri";
import { HelpInfo, StyledTooltip } from "../Tooltip";

const RootDiv = styled("div")(({theme}) => ({
  display: "flex",
  flexDirection: "column",
}));

const LegendRootDiv = styled("div")(({theme}) => ({
  display: "flex",
  flexDirection: "row",
}));

const LegendItemContainerDiv = styled("div")(({theme}) => ({
  display: "flex",
  flexDirection: "row",
  flexWrap: "nowrap",
  alignItems: "center",
  "&:not(:first-of-type)": {
    marginLeft: theme.spacing(1.5),
  },
  "&:not(:last-child)": {
    marginRight: theme.spacing(1.5),
  },
}));

const ColorLegendDiv = styled("div")(({theme}) => ({
  width: 16,
  height: 16,
  borderRadius: 4,
  marginRight: theme.spacing(1),
}));

const HintHelpInfo = styled(HelpInfo)(({theme}) => ({
  marginLeft: theme.spacing(0.5),
}));

const ProgressBarContainer = styled("div")(({theme}) => ({
  display: "flex",
  flexDirection: "row",
  alignItems: "center",
}));

const RiArrowDownSLine16 = styled(RiArrowDownSLine)(({theme}) => ({
  width: 16,
  height: 16,
  marginRight: theme.spacing(1),
}));

const RiArrowRightSLine16 = styled(RiArrowRightSLine)(({theme}) => ({
  width: 16,
  height: 16,
  marginRight: theme.spacing(1),
}));

const ProgressBarRoot = styled("div")(({theme}) => ({
  display: "flex",
  flexDirection: "row",
  flexWrap: "nowrap",
  width: "100%",
  height: 8,
  backgroundColor: "white",
  borderRadius: 6,
  overflow: "hidden",
}));

const SegmentSpan = styled("span")(({theme}) => ({
  "&:not(:last-child)": {
    marginRight: 1,
  },
}));

const ProgressTotal = styled("div")(({theme}) => ({
  flex: "1 0 40px",
  marginLeft: theme.spacing(1),
  textAlign: "end",
  whiteSpace: "nowrap",
}));

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
    <RootDiv>
      {(showLegend || controls) && (
        <Box
          display="flex"
          justifyContent="space-between"
          alignItems="center"
          marginBottom={1}
        >
          {showLegend && (
            <LegendRootDiv>
              <LegendItemContainerDiv>
                <ColorLegendDiv style={{ backgroundColor: "black" }} />
                <Typography>Total: {finalTotal}</Typography>
              </LegendItemContainerDiv>
              {filteredSegments.map(({ value, label, hint, color }) => (
                <LegendItemContainerDiv key={label}>
                  <ColorLegendDiv style={{ backgroundColor: color }}/>
                  <Typography>
                    {label}: {value}
                  </Typography>
                  {hint && <HintHelpInfo>{hint}</HintHelpInfo>}
                </LegendItemContainerDiv>
              ))}
            </LegendRootDiv>
          )}
          {controls && controls}
        </Box>
      )}
      <ProgressBarContainer onClick={onClick}>
        {expanded !== undefined &&
          (expanded ? (
            <RiArrowDownSLine16 />
          ) : (
            <RiArrowRightSLine16 />
          ))}
        <LegendTooltip
          showTooltip={showTooltip}
          total={finalTotal}
          segments={filteredSegments}
        >
          <ProgressBarRoot
            style={{
              backgroundColor: segmentTotal === 0 ? "lightGrey" : "white",
            }}
          >
            {filteredSegments.map(({ color, label, value }) => (
              <SegmentSpan
                key={label}
                style={{
                  flex: value,
                  backgroundColor: color,
                }}
                data-testid="progress-bar-segment"
              />
            ))}
          </ProgressBarRoot>
        </LegendTooltip>
        {showTotalProgress !== undefined && (
          <ProgressTotal>
            {showTotalProgress} / {finalTotal}
          </ProgressTotal>
        )}
      </ProgressBarContainer>
    </RootDiv>
  );
};

const LegendTooltipItemContainer = styled("div")(({theme}) => ({
  display: "flex",
  flexDirection: "row",
  flexWrap: "nowrap",
  alignItems: "center",
  "&:not(:first-of-type)": {
    marginTop: theme.spacing(1),
  },
}));

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
            <LegendTooltipItemContainer>
              <ColorLegendDiv
                style={{ backgroundColor: "black" }}
              />
              <Typography>Total: {total}</Typography>
            </LegendTooltipItemContainer>
            {segments.map(({ value, label, color }) => (
              <LegendTooltipItemContainer key={label}>
                <ColorLegendDiv
                  style={{ backgroundColor: color }}
                />
                <Typography>
                  {label}: {value}
                </Typography>
              </LegendTooltipItemContainer>
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
