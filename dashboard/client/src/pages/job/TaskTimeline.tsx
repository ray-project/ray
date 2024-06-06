import { Button, Link, Typography } from "@mui/material";
import { styled } from "@mui/material/styles";
import React from "react";
import { RiDownload2Line } from "react-icons/ri";
import { ClassNameProps } from "../../common/props";
import { downloadTaskTimelineHref } from "../../service/task";

type TaskTimelineProps = {
  jobId: string;
};

export const TaskTimeline = ({ jobId }: TaskTimelineProps) => {

  return (
    <div>
      {/* TODO(aguo): Add link to external documentation about Timeline view. */}
      <Typography>
        Timeline view shows how tasks are executed across different nodes and
        worker processes.
        <br />
        Download the trace file and analyze it by uploading it to tools like{" "}
        <Link href="https://ui.perfetto.dev/" target="_blank" rel="noreferrer">
          Perfetto UI
        </Link>{" "}
        or if you are using chrome,{" "}
        <Link href="chrome://tracing">chrome://tracing</Link>. You can use the
        tool by visiting chrome://tracing using your address bar.
      </Typography>
      <StyledTimelineDownloadButton jobId={jobId} />
    </div>
  );
};

type TimelineDownloadButtonProps = {
  jobId: string;
} & ClassNameProps;

const TimelineDownloadButton = ({
  jobId,
  className,
}: TimelineDownloadButtonProps) => {
  return (
    <Button
      className={className}
      variant="outlined"
      startIcon={<RiDownload2Line />}
      href={downloadTaskTimelineHref(jobId)}
    >
      Download trace file
    </Button>
  );
};

const StyledTimelineDownloadButton = styled(TimelineDownloadButton)(({theme}) => ({
  marginTop: theme.spacing(2),
}));
