import {
  createStyles,
  fade,
  Theme,
  Typography,
  withStyles,
  WithStyles,
} from "@material-ui/core";
import React from "react";
import { ErrorsByPid, getErrors } from "../../../../../api";
import DialogWithTitle from "../../../../../common/DialogWithTitle";
import NumberedLines from "../../../../../common/NumberedLines";

const styles = (theme: Theme) =>
  createStyles({
    header: {
      lineHeight: 1,
      marginBottom: theme.spacing(3),
      marginTop: theme.spacing(3),
    },
    error: {
      backgroundColor: fade(theme.palette.error.main, 0.04),
      borderLeftColor: theme.palette.error.main,
      borderLeftStyle: "solid",
      borderLeftWidth: 2,
      marginTop: theme.spacing(3),
      padding: theme.spacing(2),
    },
    timestamp: {
      color: theme.palette.text.secondary,
      marginBottom: theme.spacing(1),
    },
  });

type Props = {
  clearErrorDialog: () => void;
  nodeIp: string;
  pid: number | null;
};

type State = {
  result: ErrorsByPid | null;
  error: string | null;
};

class Errors extends React.Component<Props & WithStyles<typeof styles>, State> {
  state: State = {
    result: null,
    error: null,
  };

  async componentDidMount() {
    try {
      const { nodeIp, pid } = this.props;
      const result = await getErrors(nodeIp, pid);
      this.setState({ result: result.errors, error: null });
    } catch (error) {
      this.setState({ result: null, error: error.toString() });
    }
  }

  render() {
    const { classes, clearErrorDialog, nodeIp } = this.props;
    const { result, error } = this.state;

    return (
      <DialogWithTitle handleClose={clearErrorDialog} title="Errors">
        {error !== null ? (
          <Typography color="error">{error}</Typography>
        ) : result === null ? (
          <Typography color="textSecondary">Loading...</Typography>
        ) : (
          Object.entries(result).map(([pid, errors]) => (
            <React.Fragment key={pid}>
              <Typography className={classes.header}>
                {nodeIp} (PID: {pid})
              </Typography>
              {errors.length > 0 ? (
                errors.map(({ message, timestamp }, index) => (
                  <div className={classes.error} key={index}>
                    <Typography className={classes.timestamp}>
                      Error at {new Date(timestamp * 1000).toLocaleString()}
                    </Typography>
                    <NumberedLines lines={message.trim().split("\n")} />
                  </div>
                ))
              ) : (
                <Typography color="textSecondary">No errors found.</Typography>
              )}
            </React.Fragment>
          ))
        )}
      </DialogWithTitle>
    );
  }
}

export default withStyles(styles)(Errors);
