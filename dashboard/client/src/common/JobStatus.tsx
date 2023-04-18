import { Box, createStyles, makeStyles } from "@material-ui/core";
import classNames from "classnames";
import React from "react";
import {
  RiCheckboxCircleFill,
  RiCloseCircleFill,
  RiLoader4Line,
} from "react-icons/ri";
import { StatusChip } from "../components/StatusChip";
import { UnifiedJob } from "../type/job";

const useJobRunningIconStyles = makeStyles((theme) =>
  createStyles({
    icon: {
      width: 24,
      height: 24,
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
    iconRunning: {
      color: "#1E88E5",
      animationName: "$spinner",
      animationDuration: "1000ms",
      animationIterationCount: "infinite",
      animationTimingFunction: "linear",
    },
  }),
);

export const JobRunningIcon = () => {
  const classes = useJobRunningIconStyles();
  return (
    <RiLoader4Line className={classNames(classes.icon, classes.iconRunning)} />
  );
};

const useJobStatusIconStyles = makeStyles((theme) =>
  createStyles({
    icon: {
      width: 24,
      height: 24,
      marginRight: theme.spacing(1),
      flex: "0 0 20px",
    },
    colorSuccess: {
      color: theme.palette.success.main,
    },
    colorError: {
      color: theme.palette.error.main,
    },
  }),
);

type JobStatusIconProps = {
  job: UnifiedJob;
};

export const JobStatusIcon = ({ job }: JobStatusIconProps) => {
  const classes = useJobStatusIconStyles();

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
      return <JobRunningIcon />;
  }
};

type JobStatusWithIconProps = {
  job: UnifiedJob;
};

export const JobStatusWithIcon = ({ job }: JobStatusWithIconProps) => {
  return (
    <Box display="inline-flex" alignItems="center">
      <StatusChip type="job" status={job.status} />
      <JobStatusIcon job={job} />
    </Box>
  );
};
