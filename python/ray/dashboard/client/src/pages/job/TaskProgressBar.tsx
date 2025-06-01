import { Theme, useTheme } from "@mui/material";
import React from "react";
import { ProgressBar, ProgressBarSegment } from "../../components/ProgressBar";
import { TaskProgress } from "../../type/job";

export type TaskProgressBarProps = TaskProgress & {
  showAsComplete?: boolean;
  showLegend?: boolean;
  showTooltip?: boolean;
  expanded?: boolean;
  onClick?: () => void;
  total?: number;
  controls?: JSX.Element;
};

export const TaskProgressBar = ({
  numFinished = 0,
  numRunning = 0,
  numPendingArgsAvail = 0,
  numPendingNodeAssignment = 0,
  numSubmittedToWorker = 0,
  numFailed = 0,
  numCancelled = 0,
  numUnknown = 0,
  showAsComplete = false,
  showLegend = true,
  showTooltip = true,
  expanded,
  onClick,
  total,
  controls,
}: TaskProgressBarProps) => {
  const theme = useTheme<Theme>();
  const progress: ProgressBarSegment[] = [
    {
      label: "Finished",
      value: numFinished,
      color: theme.palette.success.main,
    },
    {
      label: "Failed",
      value: numFailed,
      color: theme.palette.error.main,
    },
    {
      label: "Running",
      value: numRunning,
      color: theme.palette.primary.main,
    },
    {
      label: "Waiting for scheduling",
      value: numPendingNodeAssignment + numSubmittedToWorker,
      color: "#cfcf08",
    },
    {
      label: "Waiting for dependencies",
      value: numPendingArgsAvail,
      color: "#f79e02",
    },
    {
      label: "Cancelled",
      value: numCancelled,
      color: theme.palette.grey.A100,
    },
    {
      label: "Unknown",
      value: numUnknown,
      color: "#5f6469",
    },
  ];
  return (
    <ProgressBar
      progress={progress}
      expanded={expanded}
      showLegend={showLegend}
      showTooltip={showTooltip}
      onClick={onClick}
      showTotalProgress={numFinished}
      total={total}
      controls={controls}
    />
  );
};

export type MiniTaskProgressBarProps = TaskProgress & {
  /**
   * Whether to color the entire progress bar as complete.
   * For example, when the job is complete.
   */
  showAsComplete?: boolean;
  /**
   * Whether to show tooltip.
   */
  showTooltip?: boolean;
  /**
   * Whether to show the total finished to the right of the progress bar.
   */
  showTotal?: boolean;
};

export const MiniTaskProgressBar = ({
  numFinished = 0,
  numRunning = 0,
  numPendingArgsAvail = 0,
  numPendingNodeAssignment = 0,
  numSubmittedToWorker = 0,
  numUnknown = 0,
  numFailed = 0,
  showAsComplete = false,
  showTooltip = true,
  showTotal = false,
}: MiniTaskProgressBarProps) => {
  const theme = useTheme<Theme>();
  if (showAsComplete) {
    const total =
      numFinished +
      numRunning +
      numPendingArgsAvail +
      numPendingNodeAssignment +
      numSubmittedToWorker +
      numFailed +
      numUnknown;
    return (
      <ProgressBar
        progress={[
          {
            label: "Finished",
            value: total - numFailed,
            color: theme.palette.success.main,
          },
          {
            label: "Failed",
            value: numFailed,
            color: theme.palette.error.main,
          },
        ]}
        showLegend={false}
        showTooltip={showTooltip}
      />
    );
  } else {
    const progress: ProgressBarSegment[] = [
      {
        label: "Finished",
        value: numFinished,
        color: theme.palette.success.main,
      },
      {
        label: "Failed",
        value: numFailed,
        color: theme.palette.error.main,
      },
      {
        label: "Running",
        value: numRunning,
        color: theme.palette.primary.main,
      },
      {
        label: "Waiting for scheduling",
        value: numPendingNodeAssignment + numSubmittedToWorker,
        color: "#cfcf08",
      },
      {
        label: "Waiting for dependencies",
        value: numPendingArgsAvail,
        color: "#f79e02",
      },
      {
        label: "Unknown",
        value: numUnknown,
        color: "#5f6469",
      },
    ];
    return (
      <ProgressBar
        progress={progress}
        showLegend={false}
        showTooltip={showTooltip}
        showTotalProgress={showTotal ? numFinished : undefined}
      />
    );
  }
};
