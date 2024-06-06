import { Box } from "@mui/material";
import { styled } from "@mui/material/styles";
import React from "react";
import { IconBaseProps } from "react-icons";
import {
  RiCheckboxCircleFill,
  RiCloseCircleFill,
  RiLoader4Line,
  RiStopCircleFill,
} from "react-icons/ri";
import { StatusChip } from "../components/StatusChip";
import { JobStatus, UnifiedJob } from "../type/job";
import { ClassNameProps } from "./props";

const iconSmall = {
  width: 16,
  height: 16,
};

const JobRunningIconType = styled(RiLoader4Line)(({ theme }) => ({
  width: 20,
  height: 20,
  animation: `spinner 2s linear infinite`,
  color: "#1E88E5",
  animationName: "$spinner",
  animationDuration: "1000ms",
  animationIterationCount: "infinite",
  animationTimingFunction: "linear",
  "@keyframes spinner": {
    from: {
      transform: "rotate(0deg)",
    },
    to: {
      transform: "rotate(360deg)",
    },
  },
}));

type JobRunningIconProps = { small?: boolean } & ClassNameProps & IconBaseProps;

export const JobRunningIcon = ({
  className,
  small = false,
  ...props
}: JobRunningIconProps) => {
  return (
    <JobRunningIconType
      className={className}
      sx={[small && iconSmall]}
      {...props}
    />
  );
};

const JobSuccessIconElement = styled(RiCheckboxCircleFill)(({ theme }) => ({
  width: 20,
  height: 20,
  color: theme.palette.success.main,
}));

const JobErrorIconElement = styled(RiCloseCircleFill)(({ theme }) => ({
  width: 20,
  height: 20,
  color: theme.palette.error.main,
}));

const JobStoppedIconElement = styled(RiStopCircleFill)(({ theme }) => ({
  width: 20,
  height: 20,
  color: "#757575",
}));

type JobStatusIconProps = {
  job: UnifiedJob;
  small?: boolean;
} & ClassNameProps;

export const JobStatusIcon = ({
  job,
  small = false,
  className,
}: JobStatusIconProps) => {
  switch (job.status) {
    case JobStatus.SUCCEEDED:
      return (
        <JobSuccessIconElement
          className={className}
          sx={[small && iconSmall]}
        />
      );
    case JobStatus.FAILED:
      return (
        <JobErrorIconElement className={className} sx={[small && iconSmall]} />
      );
    case JobStatus.STOPPED:
      return (
        <JobStoppedIconElement
          className={className}
          sx={[small && iconSmall]}
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
