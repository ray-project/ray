import { createStyles, makeStyles, Typography } from "@material-ui/core";
import classNames from "classnames";
import _ from "lodash";
import React from "react";
import {
  RiCheckboxCircleFill,
  RiCloseCircleFill,
  RiLoader4Line,
} from "react-icons/ri";
import { Link } from "react-router-dom";
import { UnifiedJob } from "../../../type/job";
import { useJobList } from "../../job/hook/useJobList";
import { OverviewCard } from "./OverviewCard";

const useStyles = makeStyles((theme) =>
  createStyles({
    root: {
      display: "flex",
      flexDirection: "column",
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
    viewAllJobs: {
      color: "#036DCF",
      textDecoration: "none",
    },
  }),
);

export const RecentJobsCard = () => {
  const classes = useStyles();

  const { jobList } = useJobList();
  const sortedJobs = _.orderBy(jobList, ["startTime"], ["desc"]).slice(0, 5);

  return (
    <OverviewCard className={classes.root}>
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
      <Link className={classes.viewAllJobs} to="/new/jobs">
        <Typography variant="h4">View all jobs →</Typography>
      </Link>
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
    icon: {
      width: 20,
      height: 20,
      marginRight: theme.spacing(1),
      flex: "0 0 20px",
    },
    "@keyframes spinner": {
      from: {
        transform: "rotate(0deg)",
      },
      to: {
        transform: "rotate(360deg)",
      },
    },
    colorSuccess: {
      color: theme.palette.success.main,
    },
    colorError: {
      color: theme.palette.error.main,
    },
    iconRunning: {
      color: "#1E88E5",
      animationName: "$spinner",
      animationDuration: "1000ms",
      animationIterationCount: "infinite",
      animationTimingFunction: "linear",
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
    },
  }),
);

type RecentJobListItemProps = {
  job: UnifiedJob;
  className?: string;
};

const RecentJobListItem = ({ job, className }: RecentJobListItemProps) => {
  const classes = useRecentJobListItemStyles();

  const icon = (() => {
    switch (job.status) {
      case "SUCCEEDED":
        return (
          <RiCheckboxCircleFill
            className={classNames(classes.icon, classes.colorSuccess)}
          />
        );
      case "FAILED":
      case "STOPPED":
        return (
          <RiCloseCircleFill
            className={classNames(classes.icon, classes.colorError)}
          />
        );
      default:
        return (
          <RiLoader4Line
            className={classNames(classes.icon, classes.iconRunning)}
          />
        );
    }
  })();
  return (
    <div className={className}>
      <Link className={classes.root} to={`/new/jobs/${job.job_id}`}>
        {icon}
        <div className={classes.textContainer}>
          <Typography className={classes.title} variant="h4">
            {job.job_id ?? job.submission_id}
          </Typography>
          <Typography className={classes.entrypoint}>
            {job.entrypoint}
          </Typography>
        </div>
      </Link>
    </div>
  );
};
