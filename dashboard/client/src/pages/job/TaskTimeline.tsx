import {
  Button,
  createStyles,
  makeStyles,
  Typography,
} from "@material-ui/core";
import React from "react";
import { RiDownload2Line } from "react-icons/ri";
import { ClassNameProps } from "../../common/props";
import { downloadTaskTimelineHref } from "../../service/task";

const useStyle = makeStyles((theme) => ({
  button: {
    marginTop: theme.spacing(2),
  },
}));

type TaskTimelineProps = {
  jobId: string;
};

export const TaskTimeline = ({ jobId }: TaskTimelineProps) => {
  const classes = useStyle();

  return (
    <div>
      {/* TODO(aguo): Add link to external documentation about Timeline view. */}
      <Typography>
        Timeline view shows how tasks are executed across different nodes and
        worker processes.
        <br />
        Download the trace file and analyze it by uploading it to tools like{" "}
        <a href="https://ui.perfetto.dev/" target="_blank" rel="noreferrer">
          Perfetto UI
        </a>{" "}
        or if you are using chrome,{" "}
        <a href="chrome://tracing">chrome://tracing</a>. You can use the tool by
        visiting chrome://tracing using your address bar.
      </Typography>
      <TimelineDownloadButton className={classes.button} jobId={jobId} />
    </div>
  );
};

const useTimelineDownloadButtonStyles = makeStyles((theme) =>
  createStyles({
    label: {
      color: "black",
    },
  }),
);

type TimelineDownloadButtonProps = {
  jobId: string;
} & ClassNameProps;

const TimelineDownloadButton = ({
  jobId,
  className,
}: TimelineDownloadButtonProps) => {
  const classes = useTimelineDownloadButtonStyles();
  return (
    <Button
      className={className}
      variant="outlined"
      startIcon={<RiDownload2Line />}
      href={downloadTaskTimelineHref(jobId)}
      classes={{ label: classes.label }}
    >
      Download trace file
    </Button>
  );
};
