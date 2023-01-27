import { Button, CircularProgress, Grid, makeStyles } from "@material-ui/core";
import dayjs from "dayjs";
import React, { useState } from "react";
import { DownloadTaskTimeline } from "../../service/task";

const useStyle = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(2),
  },
  taskProgressTable: {
    marginTop: theme.spacing(2),
  },
}));

export const TaskTimeline = ({ jobId = null }: { jobId: string | null }) => {
  const classes = useStyle();

  const TimelineDownloadButton = ({
    jobId = null,
  }: {
    jobId: string | null;
  }) => {
    const [loading, setLoading] = useState(false);

    const onClick = () => {
      // Download the chrome tracing file.
      if (!loading) {
        setLoading(true);
        // blob response type is necessary to
        DownloadTaskTimeline(jobId)
          .then((blob) => {
            const url = window.URL.createObjectURL(blob.data);
            const a = document.createElement("a");
            a.href = url;
            a.download = `timeline-${dayjs().format()}-${jobId}.json`;
            a.click();
            a.remove();
          })
          .catch((error) => {
            console.log(
              `Failed to fetch data for the timeline. Error: ${error}`,
            );
          })
          .finally(() => {
            setLoading(false);
          });
      }
    };

    if (loading) {
      return (
        <Button variant="outlined" disabled={loading}>
          Download Timeline <CircularProgress color="primary" size={16} />
        </Button>
      );
    } else {
      return (
        <Button variant="outlined" disabled={loading} onClick={onClick}>
          Download Timeline
        </Button>
      );
    }
  };

  return (
    <div className={classes.root}>
      <Grid container alignItems="center" spacing={4}>
        <Grid item>
          Timeline view shows how tasks are executed across differnt nodes and
          worker processes. Download the trace file and analyze it by uploading
          it to <a href="https://ui.perfetto.dev/">Perfetto UI</a> or{" "}
          <a href="chrome://tracing">chrome://tracing</a>.
        </Grid>
      </Grid>
      <Grid container alignItems="center" spacing={4}>
        <Grid item>
          <TimelineDownloadButton jobId={jobId} />
        </Grid>
      </Grid>
    </div>
  );
};
