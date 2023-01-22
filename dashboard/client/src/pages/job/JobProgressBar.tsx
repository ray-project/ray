import { makeStyles } from "@material-ui/core";
import React, { useEffect, useState } from "react";
import { UnifiedJob } from "../../type/job";
import { AdvancedProgressBar } from "./AdvancedProgressBar";
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
};

export const JobProgressBar = ({ jobId, job }: JobProgressBarProps) => {
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
    driverExists,
    latestFetchTimestamp: progressTimestamp,
  } = useJobProgress(jobId, advancedProgressBarExpanded);
  const {
    progressGroups,
    total,
    latestFetchTimestamp: totalTimestamp,
  } = useJobProgressByLineage(
    advancedProgressBarRendered ? jobId : undefined,
    !advancedProgressBarExpanded,
  );

  if (!driverExists) {
    return <TaskProgressBar />;
  }
  const { status } = job;
  // Use whichever data was received the most recently
  const totalProgress = progressTimestamp > totalTimestamp ? progress : total;

  return (
    <div>
      <TaskProgressBar
        {...totalProgress}
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
        />
      )}
    </div>
  );
};
