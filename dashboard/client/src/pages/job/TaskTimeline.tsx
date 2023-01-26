import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Button,
  CircularProgress,
  Grid,
  List,
  ListItem,
  makeStyles,
  Typography,
} from "@material-ui/core";
import ExpandMoreIcon from "@material-ui/icons/ExpandMore";
import dayjs from "dayjs";
import React, { useState } from "react";
import { AlertDialog } from "../../common/AlertDialog";
import { DownloadTaskTimeline, getTaskTimeline } from "../../service/task";

const PERFETTO_URL = "https://ui.perfetto.dev";
const PERFETTO_LOADING_TIMEOUT_MS = 5000;

const useStyle = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(2),
  },
  taskProgressTable: {
    marginTop: theme.spacing(2),
  },
}));

// The module guarantees there's only one fetch at
// a time meaning there can be only one timer at a time.
let timer: null | any = undefined;

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
        <div>
          <Button variant="outlined" disabled={loading}>
            Download Timeline <CircularProgress color="primary" size={16} />
          </Button>
        </div>
      );
    } else {
      return (
        <Button variant="outlined" disabled={loading} onClick={onClick}>
          Download Timeline
        </Button>
      );
    }
  };

  const TimelineButton = ({ jobId = null }: { jobId: string | null }) => {
    const [loading, setLoading] = useState(false);
    const [openDialog, setOpenDialog] = useState(false);

    // Code from https://perfetto.dev/docs/visualization/deep-linking-to-perfetto-ui
    const openTrace = (arrayBuffer: any) => {
      const win = window.open(PERFETTO_URL);
      const start = new Date().getTime();
      if (win) {
        window.addEventListener("message", (evt) =>
          onMessage(evt, win, arrayBuffer, start),
        );
        timer = setInterval(() => win.postMessage("PING", PERFETTO_URL), 50);
      }
    };

    const onMessage = (evt: any, win: any, arrayBuffer: any, start: any) => {
      if (new Date().getTime() - start > PERFETTO_LOADING_TIMEOUT_MS) {
        // If perfetto doesn't respond in 5 seconds, fail it.
        setLoading(false);
        return;
      }

      if (evt.data !== "PONG") {
        return;
      }

      window.clearInterval(timer);
      win.postMessage(arrayBuffer, PERFETTO_URL);
      // We now finished uploading timeline.
      setLoading(false);
    };

    const onDialogClose = () => {
      setOpenDialog(false);
    };

    const onClick = () => {
      setOpenDialog(true);
    };

    const onClickPerfettoLoading = () => {
      if (!loading) {
        setLoading(true);
        // Instead of downloading a json file, it posts the result to
        // perfetto directly.
        getTaskTimeline(jobId)
          .then((resp) => {
            openTrace(resp.data);
          })
          .catch((error) => {
            setLoading(false);
            console.log(
              `Failed to fetch data for the timeline. Error: ${error}`,
            );
          });
      }
      setOpenDialog(false);
    };

    // TODO(sang): Raise a error message when fetch fails.
    if (openDialog) {
      return (
        <AlertDialog
          open={openDialog}
          onAgree={onClickPerfettoLoading}
          handleClose={onDialogClose}
          title={"View on ui.perfetto.dev (a Google service)"}
          contents={
            "Timeline tracing will be uploaded to ui.perfetto.dev for tracing visualization. According to https://perfetto.dev/docs/#trace-visualization, the data is not suppposed to be uploaded to their server."
          }
        />
      );
    } else if (loading) {
      return (
        <div>
          <Button variant="outlined" disabled={loading}>
            Open Timeline View <CircularProgress color="primary" size={16} />
          </Button>
        </div>
      );
    } else {
      return (
        <Button variant="outlined" disabled={loading} onClick={onClick}>
          Open Timeline View
        </Button>
      );
    }
  };

  return (
    <div className={classes.root}>
      <Grid container alignItems="center" spacing={4}>
        <Grid item>
          Timeline view shows how many tasks are scheduled and executed across
          the cluster over time.
        </Grid>
      </Grid>
      <Grid container alignItems="center" spacing={4}>
        <Grid item>
          <TimelineDownloadButton jobId={jobId} />
        </Grid>
        <Grid item>
          <TimelineButton jobId={jobId} />
        </Grid>
      </Grid>
      <Grid container alignItems="center" spacing={2}>
        <Grid item>
          <Accordion>
            <AccordionSummary
              expandIcon={<ExpandMoreIcon />}
              aria-controls="panel1a-content"
              id="panel1a-header"
            >
              <Typography variant="h6">Instruction</Typography>
            </AccordionSummary>
            <AccordionDetails>
              <Typography variant="subtitle1">
                <List>
                  <ListItem>
                    - Download the chrome tracing file by clicking "Download
                    timeline". It will download the chrome tracing json file.
                  </ListItem>
                  <ListItem>
                    - Either go to "chrome://tracing" or
                    "https://ui.perfetto.dev/" and upload the tracing file.
                  </ListItem>
                  <ListItem>
                    -{" "}
                    <b>
                      If you click the "Open Timeline View", it will
                      automatically upload timeline json file to
                      "https://ui.perfetto.dev/"
                    </b>
                    .
                  </ListItem>
                </List>
              </Typography>
            </AccordionDetails>
          </Accordion>
        </Grid>
      </Grid>
    </div>
  );
};
