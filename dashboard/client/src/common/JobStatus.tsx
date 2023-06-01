import { Box, createStyles, makeStyles } from "@material-ui/core";
import classNames from "classnames";
import React from "react";
import {
  RiCheckboxCircleFill,
  RiCloseCircleFill,
  RiLoader4Line,
  RiStopCircleFill,
} from "react-icons/ri";
import { StatusChip } from "../components/StatusChip";
import { JobStatus, UnifiedJob } from "../type/job";
import { ClassNameProps } from "./props";

const useJobRunningIconStyles = makeStyles((theme) =>
  createStyles({
    icon: {
      width: 20,
      height: 20,
    },
    iconSmall: {
      width: 16,
      height: 16,
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

type JobRunningIconProps = { small?: boolean } & ClassNameProps;

export const JobRunningIcon = ({
  className,
  small = false,
}: JobRunningIconProps) => {
  const classes = useJobRunningIconStyles();
  return (
    <RiLoader4Line
      className={classNames(
        classes.icon,
        classes.iconRunning,
        {
          [classes.iconSmall]: small,
        },
        className,
      )}
    />
  );
};

const useJobStatusIconStyles = makeStyles((theme) =>
  createStyles({
    icon: {
      width: 20,
      height: 20,
    },
    iconSmall: {
      width: 16,
      height: 16,
    },
    colorSuccess: {
      color: theme.palette.success.main,
    },
    colorError: {
      color: theme.palette.error.main,
    },
    colorStopped: {
      color: "#757575",
    },
  }),
);

type JobStatusIconProps = {
  job: UnifiedJob;
  small?: boolean;
} & ClassNameProps;

export const JobStatusIcon = ({
  job,
  small = false,
  className,
}: JobStatusIconProps) => {
  const classes = useJobStatusIconStyles();
  switch (job.status) {
    case JobStatus.SUCCEEDED:
      return (
        <RiCheckboxCircleFill
          className={classNames(
            classes.icon,
            classes.colorSuccess,
            {
              [classes.iconSmall]: small,
            },
            className,
          )}
        />
      );
    case JobStatus.FAILED:
      return (
        <RiCloseCircleFill
          className={classNames(
            classes.icon,
            classes.colorError,
            {
              [classes.iconSmall]: small,
            },
            className,
          )}
        />
      );
    case JobStatus.STOPPED:
      return (
        <RiStopCircleFill
          className={classNames(
            classes.icon,
            classes.colorStopped,
            {
              [classes.iconSmall]: small,
            },
            className,
          )}
        />
      );
    default:
      return <JobRunningIcon className={className} small={small} />;
  }
};

type JobStatusWithIconProps = {
  job: UnifiedJob;
};

export const JobStatusWithIcon = ({ job }: JobStatusWithIconProps) => {
  return (
    <Box display="inline-flex" alignItems="center">
      <StatusChip
        type="job"
        status={job.status}
        icon={job.status === JobStatus.RUNNING && <JobRunningIcon />}
      />
    </Box>
  );
};
