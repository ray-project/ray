import { Checkbox, FormControlLabel, LinearProgress } from "@mui/material";
import React, { useEffect, useState } from "react";
import { UnifiedJob } from "../../type/job";
import {
  AdvancedProgressBar,
  AdvancedProgressBarProps,
} from "./AdvancedProgressBar";
import { useJobProgress, useJobProgressByLineage } from "./hook/useJobProgress";
import { TaskProgressBar } from "./TaskProgressBar";

type JobProgressBarProps = {
  jobId: string | undefined;
  job: Pick<UnifiedJob, "status">;
} & Pick<AdvancedProgressBarProps, "onClickLink">;

export const JobProgressBar = ({
  jobId,
  job,
  ...advancedProgressBarProps
}: JobProgressBarProps) => {
  // Controls the first time we fetch the advanced progress bar data
  const [advancedProgressBarRendered, setAdvancedProgressBarRendered] =
    useState(false);
  // Controls whether we continue to fetch the advanced progress bar data
  const [advancedProgressBarExpanded, setAdvancedProgressBarExpanded] =
    useState(false);

  const [showFinishedTasks, setShowFinishedTasks] = useState(true);

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
    showFinishedTasks,
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
        controls={
          <FormControlLabel
            control={
              <Checkbox
                color="primary"
                value={!showFinishedTasks}
                onChange={({ target: { checked } }) => {
                  setShowFinishedTasks(!checked);
                }}
              />
            }
            label="Hide finished"
            sx={{ marginRight: 0 }}
          />
        }
      />
      {advancedProgressBarExpanded && (
        <AdvancedProgressBar
          sx={{ marginTop: 0.5 }}
          progressGroups={progressGroups}
          {...advancedProgressBarProps}
        />
      )}
    </div>
  );
};
