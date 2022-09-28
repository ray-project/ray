import { Theme } from "@material-ui/core";
import { useTheme } from "@material-ui/styles";
import React from "react";
import { ProgressBar, ProgressBarSegment } from "../../components/ProgressBar";
import { TaskProgress } from "../../type/job";

export type TaskProgressBarProps = TaskProgress;

export const TaskProgressBar = ({
  numFinished = 0,
  numRunning = 0,
  numPendingArgsAvail = 0,
  numPendingNodeAssignment = 0,
  numSubmittedToWorker = 0,
  numUnknown = 0,
}: TaskProgressBarProps) => {
  const theme = useTheme<Theme>();
  const total =
    numFinished +
    numRunning +
    numPendingArgsAvail +
    numPendingNodeAssignment +
    numSubmittedToWorker +
    numUnknown;
  const progress: ProgressBarSegment[] = [
    {
      label: "Finished",
      value: numFinished,
      color: theme.palette.success.main,
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
  ];
  return (
    <ProgressBar
      progress={progress}
      total={total}
      unaccountedLabel="Scheduled"
    />
  );
};

export const MiniTaskProgressBar = ({
  numFinished = 0,
  numRunning = 0,
  numPendingArgsAvail = 0,
  numPendingNodeAssignment = 0,
  numSubmittedToWorker = 0,
  numUnknown = 0,
}: TaskProgressBarProps) => {
  const theme = useTheme<Theme>();
  const total =
    numFinished +
    numRunning +
    numPendingArgsAvail +
    numPendingNodeAssignment +
    numSubmittedToWorker +
    numUnknown;
  const progress: ProgressBarSegment[] = [
    {
      label: "Finished",
      value: numFinished,
      color: theme.palette.success.main,
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
  ];
  return (
    <ProgressBar
      progress={progress}
      total={total}
      unaccountedLabel="Scheduled"
      showLegend={false}
    />
  );
};
