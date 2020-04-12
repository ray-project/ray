import {
  createStyles,
  fade,
  Theme,
  Typography,
  WithStyles,
  withStyles,
} from "@material-ui/core";
import React from "react";
import { getLogs, LogsResponse } from "../../../../../api";
import DialogWithTitle from "../../../../../common/DialogWithTitle";
import NumberedLines from "../../../../../common/NumberedLines";

const styles = (theme: Theme) =>
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
  });

type Props = {
  clearLogDialog: () => void;
  hostname: string;
  pid: number | null;
};

type State = {
  result: LogsResponse | null;
  error: string | null;
};

class Logs extends React.Component<Props & WithStyles<typeof styles>, State> {
  state: State = {
    result: null,
    error: null,
  };

  async componentDidMount() {
    try {
      const { hostname, pid } = this.props;
      const result = await getLogs(hostname, pid);
      this.setState({ result, error: null });
    } catch (error) {
      this.setState({ result: null, error: error.toString() });
    }
  }

  render() {
    const { classes, clearLogDialog, hostname } = this.props;
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
                {hostname} (PID: {pid})
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
