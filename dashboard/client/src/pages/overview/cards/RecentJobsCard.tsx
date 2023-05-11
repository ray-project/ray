import { createStyles, makeStyles, Typography } from "@material-ui/core";
import classNames from "classnames";
import _ from "lodash";
import React from "react";
import { Link } from "react-router-dom";
import { JobStatusIcon } from "../../../common/JobStatus";
import { UnifiedJob } from "../../../type/job";
import { useJobList } from "../../job/hook/useJobList";
import { LinkWithArrow, OverviewCard } from "./OverviewCard";

const useStyles = makeStyles((theme) =>
  createStyles({
    root: {
      display: "flex",
      flexDirection: "column",
      padding: theme.spacing(2, 3),
    },
    listContainer: {
      marginTop: theme.spacing(2),
      flex: 1,
      overflow: "hidden",
    },
    listItem: {
      "&:not(:first-child)": {
        marginTop: theme.spacing(1),
      },
    },
  }),
);

type RecentJobsCardProps = {
  className?: string;
};

export const RecentJobsCard = ({ className }: RecentJobsCardProps) => {
  const classes = useStyles();

  const { jobList } = useJobList();

  const sortedJobs = _.orderBy(jobList, ["startTime"], ["desc"]).slice(0, 6);

  return (
    <OverviewCard className={classNames(classes.root, className)}>
      <Typography variant="h3">Recent jobs</Typography>
      <div className={classes.listContainer}>
        {sortedJobs.map((job) => (
          <RecentJobListItem
            key={job.job_id ?? job.submission_id}
            className={classes.listItem}
            job={job}
          />
        ))}
        {sortedJobs.length === 0 && (
          <Typography variant="h4">No jobs yet...</Typography>
        )}
      </div>
      <LinkWithArrow text="View all jobs" to="/jobs" />
    </OverviewCard>
  );
};

const useRecentJobListItemStyles = makeStyles((theme) =>
  createStyles({
    root: {
      display: "flex",
      flexDirection: "row",
      flexWrap: "nowrap",
      alignItems: "center",
      textDecoration: "none",
    },
    textContainer: {
      flex: "1 1 auto",
      width: `calc(100% - ${theme.spacing(1) + 20}px)`,
    },
    title: {
      color: "#036DCF",
    },
    entrypoint: {
      overflow: "hidden",
      textOverflow: "ellipsis",
      whiteSpace: "nowrap",
      color: "#5F6469",
    },
    icon: {
      marginRight: theme.spacing(1),
    },
  }),
);

type RecentJobListItemProps = {
  job: UnifiedJob;
  className?: string;
};

const RecentJobListItem = ({ job, className }: RecentJobListItemProps) => {
  const classes = useRecentJobListItemStyles();

  const cardContent = (
    <React.Fragment>
      <JobStatusIcon className={classes.icon} job={job} />
      <div className={classes.textContainer}>
        <Typography className={classes.title} variant="body2">
          {job.job_id ?? job.submission_id}
        </Typography>
        <Typography
          className={classes.entrypoint}
          title={job.entrypoint}
          variant="caption"
        >
          {job.entrypoint}
        </Typography>
      </div>
    </React.Fragment>
  );

  return (
    <div className={className}>
      {job.job_id !== null && job.job_id !== "" ? (
        <Link className={classes.root} to={`/jobs/${job.job_id}`}>
          {cardContent}
        </Link>
      ) : (
        <div className={classes.root}>{cardContent}</div>
      )}
    </div>
  );
};
