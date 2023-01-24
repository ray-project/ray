import { makeStyles, MenuItem, Select, Typography } from "@material-ui/core";
import React, { useEffect, useState } from "react";
import { UnifiedJob } from "../../type/job";
import { AdvancedProgressBar } from "./AdvancedProgressBar";
import { useJobProgress, useJobProgressByLineage } from "./hook/useJobProgress";
import { TaskProgressBar } from "./TaskProgressBar";

const useStyles = makeStyles((theme) => ({
  advancedProgressBar: {
    marginTop: theme.spacing(0.5),
  },
  summaryByContainer: {
    marginBottom: theme.spacing(1),
  },
  summaryByDropdown: {
    marginLeft: theme.spacing(0.5),
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
  const [summaryType, setSummaryType] =
    useState<"lineage" | "lineage_and_name">("lineage");

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
    summaryType,
  );

  if (!driverExists) {
    return <TaskProgressBar />;
  }
  const { status } = job;
  // Use whichever data was received the most recently
  // Note these values may disagree in some way. It might better to consistently use one endpoint.
  const totalProgress = progressTimestamp > totalTimestamp ? progress : total;

  return (
    <div>
      <Typography className={classes.summaryByContainer}>
        Summary by:
        <Select
          className={classes.summaryByDropdown}
          value={summaryType}
          onChange={({ target: { value } }) => {
            setSummaryType(value as "lineage" | "lineage_and_name");
          }}
        >
          <MenuItem value="lineage">Lineage</MenuItem>
          <MenuItem value="lineage_and_name">Lineage and name</MenuItem>
        </Select>
      </Typography>
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
