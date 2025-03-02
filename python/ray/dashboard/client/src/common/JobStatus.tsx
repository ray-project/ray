import { Cancel } from "@mui/icons-material";
import {
  Alert,
  Box,
  IconButton,
  keyframes,
  Snackbar,
  SxProps,
  Theme,
} from "@mui/material";
import React from "react";
import {
  RiCheckboxCircleFill,
  RiCloseCircleFill,
  RiLoader4Line,
  RiStopCircleFill,
} from "react-icons/ri";
import { StatusChip } from "../components/StatusChip";
import { post } from "../service/requestHandlers";
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
  killable?: boolean;
};

export const JobStatusWithIcon = ({
  job,
  killable,
}: JobStatusWithIconProps) => {
  const [open, setOpen] = React.useState(false);
  const [cancelled, setCancelled] = React.useState<string | null>("x");
  const job_or_submission_id = job.job_id || job.submission_id;
  const job_cancellable =
    killable &&
    job_or_submission_id &&
    (job.status === "RUNNING" || job.status === "PENDING");
  const handleClose = (
    event?: React.SyntheticEvent | Event,
    reason?: string,
  ) => {
    if (reason === "clickaway") {
      return;
    }
    setOpen(false);
  };
  const handleCancel = () => {
    setCancelled(null);
    if (job_cancellable) {
      post<any>(`api/jobs/${job_or_submission_id}/stop`, null, {
        validateStatus: (_) => true,
      })
        .then((resp) => {
          if (resp.status === 200) {
            if (resp.data?.stopped) {
              return "";
            }
            return "Failed to stop job";
          } else {
            return `Error Resp ${resp.status}: ${resp.data}`;
          }
        })
        .catch((e) => {
          return `${e}`;
        })
        .then((status) => {
          setCancelled(status ?? "");
          setOpen(false);
        });
      setOpen(true);
    }
  };
  return (
    <Box display="inline-flex" alignItems="center">
      <StatusChip
        type="job"
        status={job.status}
        icon={job.status === JobStatus.RUNNING && <JobRunningIcon />}
      />
      {job_cancellable && (
        <React.Fragment>
          <IconButton
            color="primary"
            aria-label="Cancel this job"
            size="small"
            onClick={handleCancel}
            disabled={!cancelled}
          >
            <Cancel /> Kill Job
          </IconButton>
          <Snackbar
            open={open}
            onClose={handleClose}
            autoHideDuration={cancelled !== null ? 5000 : null}
            anchorOrigin={{ vertical: "top", horizontal: "center" }}
          >
            {cancelled === null ? (
              <Alert severity="warning">
                Killing job {job_or_submission_id}
              </Alert>
            ) : cancelled === "" ? (
              <Alert severity="success" onClose={handleClose}>
                Job {job_or_submission_id} has been killed
              </Alert>
            ) : (
              <Alert severity="error" onClose={handleClose}>
                Failed to kill job {job_or_submission_id}: {cancelled}
              </Alert>
            )}
          </Snackbar>
        </React.Fragment>
      )}
    </Box>
  );
};
