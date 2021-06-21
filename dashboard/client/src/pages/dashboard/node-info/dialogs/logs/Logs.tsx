import {
  createStyles,
  fade,
  Theme,
  Typography,
  WithStyles,
  withStyles,
} from "@material-ui/core";
import React from "react";
import { getLogs, LogsByPid } from "../../../../../api";
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
  nodeIp: string;
  pid: number | null;
};

type State = {
  result: LogsByPid | null;
  error: string | null;
};

class Logs extends React.Component<Props & WithStyles<typeof styles>, State> {
  state: State = {
    result: null,
    error: null,
  };

  async componentDidMount() {
    try {
      const { nodeIp, pid } = this.props;
      const result = await getLogs(nodeIp, pid);
      this.setState({ result: result.logs, error: null });
    } catch (error) {
      this.setState({ result: null, error: error.toString() });
    }
  }

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
