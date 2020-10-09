import {
  createStyles,
  fade,
  Theme,
  Typography,
  makeStyles
} from "@material-ui/core";
import React, {useState, useEffect} from "react";
import { getLogs, LogsByPid } from "../../../../../api";
import DialogWithTitle from "../../../../../common/DialogWithTitle";
import NumberedLines from "../../../../../common/NumberedLines";

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
  logs: LogsByPid;
  groupTag: string;
};

const LogPane: React.FC<LogPaneProps> = ({
  logs,
  clearLogDialog,
  groupTag,
}) => {
  const classes = useLogPaneStyles();
  return (
  <DialogWithTitle handleClose={clearLogDialog} title="Logs">
    {
      Object.entries(logs).map(([pid, lines]) => (
        <React.Fragment key={pid}>
          <Typography className={classes.header}>
            {nodeIp} (PID: {pid})
          </Typography>
          {lines.length > 0 ? (
            <div className={classes.log}>
              <NumberedLines lines={lines} />
            </div>) :
            <Typography color="textSecondary">No logs found.</Typography>
            }
        )
        </React.Fragment>
      ))
    }
  </DialogWithTitle>)
};

type FetchingLogPaneProps = {
  clearLogDialog: () => void;
  nodeIp: string;
  pid: number | null;
};

const FetchingLogPane: React.FC<FetchingLogPaneProps> = ({
  clearLogDialog,
  nodeIp,
  pid
}) => {
  const [logs, setLogs] = useState<string[]>([]);
  const [error, setError] = useState<null | string>(null)
  useEffect(() => {
    try {
      const result = await getLogs(nodeIp, pid);
      setLogs(result.logs);
      setError(null);
    } catch (error) {
      setError(error.toString());
    }
  });
  return;
};

class Logs extends React.Component<Props & WithStyles<typeof styles>, State> {
  render() {
    const { classes, clearLogDialog, nodeIp } = this.props;
    const { result, error } = this.state;

    return (
      <DialogWithTitle handleClose={clearLogDialog} title="Logs">
        {error !== null ? (
          <Typography color="error">{error}</Typography>
        ) : result === null ? (
          <Typography color="textSecondary">Loading...</Typography>
        ) : (
          Object.entries(result).map(([pid, lines]) => (
            <React.Fragment key={pid}>
              <Typography className={classes.header}>
                {nodeIp} (PID: {pid})
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
      </DialogWithTitle>
    );
  }
}

export default withStyles(styles)(Logs);
