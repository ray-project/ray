import { fade } from "@material-ui/core/styles/colorManipulator";
import { Theme } from "@material-ui/core/styles/createMuiTheme";
import createStyles from "@material-ui/core/styles/createStyles";
import withStyles, { WithStyles } from "@material-ui/core/styles/withStyles";
import Typography from "@material-ui/core/Typography";
import React from "react";
import { RouteComponentProps } from "react-router";
import { getLogs, LogsResponse } from "../../../../api";
import DialogWithTitle from "../../../../common/DialogWithTitle";
import NumberedLines from "../../../../common/NumberedLines";

const styles = (theme: Theme) =>
  createStyles({
    header: {
      lineHeight: 1,
      marginBottom: theme.spacing(3),
      marginTop: theme.spacing(3)
    },
    log: {
      backgroundColor: fade(theme.palette.primary.main, 0.04),
      borderLeftColor: theme.palette.primary.main,
      borderLeftStyle: "solid",
      borderLeftWidth: 2,
      padding: theme.spacing(2)
    }
  });

interface State {
  result: LogsResponse | null;
  error: string | null;
}

class Logs extends React.Component<
  WithStyles<typeof styles> &
    RouteComponentProps<{ hostname: string; pid: string | undefined }>,
  State
> {
  state: State = {
    result: null,
    error: null
  };

  handleClose = () => {
    this.props.history.push("/");
  };

  async componentDidMount() {
    try {
      const { match } = this.props;
      const { hostname, pid } = match.params;
      const result = await getLogs(hostname, pid);
      this.setState({ result, error: null });
    } catch (error) {
      this.setState({ result: null, error: error.toString() });
    }
  }

  render() {
    const { classes, match } = this.props;
    const { result, error } = this.state;

    const { hostname } = match.params;

    return (
      <DialogWithTitle handleClose={this.handleClose} title="Logs">
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
