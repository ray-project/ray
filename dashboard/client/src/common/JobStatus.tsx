import { Box, keyframes, SxProps, Theme } from "@mui/material";
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
  return (
    <Box
      component={RiLoader4Line}
      sx={[
        {
          width: small ? 16 : 20,
          height: small ? 16 : 20,
          color: "#1E88E5",
          animation: `${spinner} 1s linear infinite`,
        },
        ...(Array.isArray(sx) ? sx : [sx]),
      ]}
      title={title}
      {...props}
    />
  );
};

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
  switch (job.status) {
    case JobStatus.SUCCEEDED:
      return (
        <Box
          component={RiCheckboxCircleFill}
          className={className}
          sx={[
            {
              width: small ? 16 : 20,
              height: small ? 16 : 20,
              color: (theme) => theme.palette.success.main,
            },
            ...(Array.isArray(sx) ? sx : [sx]),
          ]}
        />
      );
    case JobStatus.FAILED:
      return (
        <Box
          component={RiCloseCircleFill}
          className={className}
          sx={[
            {
              width: small ? 16 : 20,
              height: small ? 16 : 20,
              color: (theme) => theme.palette.error.main,
            },
            ...(Array.isArray(sx) ? sx : [sx]),
          ]}
        />
      );
    case JobStatus.STOPPED:
      return (
        <Box
          component={RiStopCircleFill}
          className={className}
          sx={[
            {
              width: small ? 16 : 20,
              height: small ? 16 : 20,
              color: "#757575",
            },
            ...(Array.isArray(sx) ? sx : [sx]),
          ]}
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
