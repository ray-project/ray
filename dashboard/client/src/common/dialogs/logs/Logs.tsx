import {
  createStyles,
  fade,
  Theme,
  Typography,
  makeStyles
} from "@material-ui/core";
import React, {useState, useEffect} from "react";
import { getLogs, LogsByPid } from "../../../api";
import DialogWithTitle from "../../DialogWithTitle";
import NumberedLines from "../../NumberedLines";

const useLogPaneStyles = makeStyles((theme: Theme) =>
  createStyles({
    header: {
      lineHeight: 1,
      marginBottom: theme.spacing(3),
      marginTop: theme.spacing(3),
    },
    log: {
      backgroundColor: fade(theme.palette.primary.main, 0.04),
      borderLeftColor: theme.palette.primary.main,
      borderLeftStyle: "solid",
      borderLeftWidth: 2,
      padding: theme.spacing(2),
    },
  }));

type LogPaneProps = {
  clearLogDialog: () => void;
  logs: LogsByPid | null;
  error: string | null;
  groupTag: string;
};

export const LogPane: React.FC<LogPaneProps> = ({
  logs,
  error,
  clearLogDialog,
  groupTag,
}) => {
  const classes = useLogPaneStyles();
  return (
    <DialogWithTitle handleClose={clearLogDialog} title="Logs">
    {error !== null ? (
      <Typography color="error">{error}</Typography>
    ) : logs === null ? (
      <Typography color="textSecondary">Loading...</Typography>
    ) : (
      Object.entries(logs).map(([pid, lines]) => (
        <React.Fragment key={pid}>
          <Typography className={classes.header}>
            {groupTag} (PID: {pid})
          </Typography>
          {lines.length > 0 ? (
            <div className={classes.log}>
              <NumberedLines lines={lines} />
            </div>
          ) : (
            <Typography color="textSecondary">No logs found.</Typography>
          )}
        </React.Fragment>
      ))
    )}
  </DialogWithTitle>)
};

type FetchingLogPaneProps = {
  clearLogDialog: () => void;
  nodeIp: string;
  pid: number | null;
};

export const FetchingLogPane: React.FC<FetchingLogPaneProps> = ({
  clearLogDialog,
  nodeIp,
  pid
}) => {
  const [logs, setLogs] = useState<LogsByPid | null>(null);
  const [error, setError] = useState<string | null>(null)
  useEffect(() => {
    (async () => {
      try {
        const result = await getLogs(nodeIp, pid);
        setLogs(result.logs);
        setError(null);
      } catch (error) {
        setLogs(null);
        setError(error.toString());
      }
    })();
  }, [nodeIp, pid]);
  return (
    <LogPane
      logs={logs}
      error={error}
      clearLogDialog={clearLogDialog}
      groupTag={nodeIp} />
  )

};

export default FetchingLogPane;