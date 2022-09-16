import { Theme } from "@material-ui/core";
import { useTheme } from "@material-ui/styles";
import React from "react";
import { ProgressBar, ProgressBarSegment } from "../../components/ProgressBar";
import { TaskProgress } from "../../type/job";

export type TaskProgressBarProps = TaskProgress;

export const TaskProgressBar = ({
  numFinished = 0,
  numRunning = 0,
  numScheduled = 0,
  numWaitingForDependencies = 0,
  numWaitingForExecution = 0,
}: TaskProgressBarProps) => {
  const theme = useTheme<Theme>();
  const total =
    numFinished +
    numRunning +
    numScheduled +
    numWaitingForDependencies +
    numWaitingForExecution;
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
      label: "Waiting for execution",
      value: numWaitingForExecution,
      color: "#cfcf08",
    },
    {
      label: "Waiting for dependencies",
      value: numWaitingForDependencies,
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
