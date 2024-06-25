import { Box, keyframes, SxProps, Theme, useTheme } from "@mui/material";
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

const spinner = keyframes`
from {
  transform: rotate(0deg)
},
to {
  transform: rotate(360deg)
}`;

const useJobRunningIconStyles = (theme: Theme) => ({
  icon: (small: boolean) => ({
    width: small ? 16 : 20,
    height: small ? 16 : 20,
  }),
  iconRunning: {
    color: "#1E88E5",
    animation: `${spinner} 1s linear infinite`,
  },
});

type JobRunningIconProps = {
  title?: string;
  small?: boolean;
  sx?: SxProps<Theme>;
} & ClassNameProps;

export const JobRunningIcon = ({
  className,
  title,
  small = false,
  sx = [],
  ...props
}: JobRunningIconProps) => {
  const styles = useJobRunningIconStyles(useTheme());
  return (
    <Box
      component={RiLoader4Line}
      sx={[
        styles.icon(small),
        styles.iconRunning,
        ...(Array.isArray(sx) ? sx : [sx]),
      ]}
      title={title}
      {...props}
    />
  );
};

const useJobStatusIconStyles = (theme: Theme) => ({
  icon: (small: boolean) => ({
    width: small ? 16 : 20,
    height: small ? 16 : 20,
  }),
  colorSuccess: {
    color: theme.palette.success.main,
  },
  colorError: {
    color: theme.palette.error.main,
  },
  colorStopped: {
    color: "#757575",
  },
});

type JobStatusIconProps = {
  job: UnifiedJob;
  small?: boolean;
  sx?: SxProps<Theme>;
} & ClassNameProps;

export const JobStatusIcon = ({
  job,
  small = false,
  className,
  sx,
}: JobStatusIconProps) => {
  const styles = useJobStatusIconStyles(useTheme());
  const sx_styles = Array.isArray(sx) ? sx : [sx];
  switch (job.status) {
    case JobStatus.SUCCEEDED:
      return (
        <Box
          component={RiCheckboxCircleFill}
          className={className}
          sx={[styles.icon(small), styles.colorSuccess, ...sx_styles]}
        />
      );
    case JobStatus.FAILED:
      return (
        <Box
          component={RiCloseCircleFill}
          className={className}
          sx={[styles.icon(small), styles.colorError, ...sx_styles]}
        />
      );
    case JobStatus.STOPPED:
      return (
        <Box
          component={RiStopCircleFill}
          className={className}
          sx={[styles.icon(small), styles.colorStopped, ...sx_styles]}
        />
      );
    default:
      return <JobRunningIcon className={className} sx={sx} small={small} />;
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
