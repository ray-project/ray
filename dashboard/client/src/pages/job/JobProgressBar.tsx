import { LinearProgress, makeStyles } from "@material-ui/core";
import React, { useEffect, useState } from "react";
import { UnifiedJob } from "../../type/job";
import {
  AdvancedProgressBar,
  AdvancedProgressBarProps,
} from "./AdvancedProgressBar";
import { useJobProgress, useJobProgressByLineage } from "./hook/useJobProgress";
import { TaskProgressBar } from "./TaskProgressBar";

const useStyles = makeStyles((theme) => ({
  advancedProgressBar: {
    marginTop: theme.spacing(0.5),
  },
}));

type JobProgressBarProps = {
  jobId: string;
  job: Pick<UnifiedJob, "status">;
} & Pick<AdvancedProgressBarProps, "onClickLink">;

export const JobProgressBar = ({
  jobId,
  job,
  ...advancedProgressBarProps
}: JobProgressBarProps) => {
  const classes = useStyles();

  // Controls the first time we fetch the advanced progress bar data
  const [advancedProgressBarRendered, setAdvancedProgressBarRendered] =
    useState(false);
  // Controls whether we continue to fetch the advanced progress bar data
  const [advancedProgressBarExpanded, setAdvancedProgressBarExpanded] =
    useState(false);

  useEffect(() => {
    if (advancedProgressBarExpanded) {
      setAdvancedProgressBarRendered(true);
    }
  }, [advancedProgressBarExpanded]);

  const {
    progress,
    isLoading: progressLoading,
    driverExists,
    totalTasks,
    latestFetchTimestamp: progressTimestamp,
  } = useJobProgress(jobId, advancedProgressBarExpanded);
  const {
    progressGroups,
    isLoading: progressGroupsLoading,
    total,
    totalTasks: advancedTotalTasks,
    latestFetchTimestamp: totalTimestamp,
  } = useJobProgressByLineage(
    advancedProgressBarRendered ? jobId : undefined,
    !advancedProgressBarExpanded,
  );

  if (!driverExists) {
    return <TaskProgressBar />;
  }

  if (
    progressLoading &&
    (progressGroupsLoading || !advancedProgressBarRendered)
  ) {
    return <LinearProgress />;
  }

  const { status } = job;
  // Use whichever data was received the most recently
  // Note these values may disagree in some way. It might better to consistently use one endpoint.
  const [totalProgress, finalTotalTasks] =
    total === undefined ||
    advancedTotalTasks === undefined ||
    progressTimestamp > totalTimestamp
      ? [progress, totalTasks]
      : [total, advancedTotalTasks];

  return (
    <div>
      <TaskProgressBar
        {...totalProgress}
        total={finalTotalTasks}
        showAsComplete={status === "SUCCEEDED" || status === "FAILED"}
        showTooltip={false}
        expanded={advancedProgressBarExpanded}
        onClick={() =>
          setAdvancedProgressBarExpanded(!advancedProgressBarExpanded)
        }
      />
      {advancedProgressBarExpanded && (
        <AdvancedProgressBar
          className={classes.advancedProgressBar}
          progressGroups={progressGroups}
          {...advancedProgressBarProps}
        />
      )}
    </div>
  );
};
