import { fade } from "@material-ui/core/styles/colorManipulator";
import { Theme } from "@material-ui/core/styles/createMuiTheme";
import createStyles from "@material-ui/core/styles/createStyles";
import withStyles, { WithStyles } from "@material-ui/core/styles/withStyles";
import Typography from "@material-ui/core/Typography";
import React from "react";
import { RouteComponentProps } from "react-router";
import { ErrorsResponse, getErrors } from "../../../../api";
import DialogWithTitle from "../../../../common/DialogWithTitle";
import NumberedLines from "../../../../common/NumberedLines";

const styles = (theme: Theme) =>
  createStyles({
    header: {
      lineHeight: 1,
      marginBottom: theme.spacing(3),
      marginTop: theme.spacing(3)
    },
    error: {
      backgroundColor: fade(theme.palette.error.main, 0.04),
      borderLeftColor: theme.palette.error.main,
      borderLeftStyle: "solid",
      borderLeftWidth: 2,
      marginTop: theme.spacing(3),
      padding: theme.spacing(2)
    },
    timestamp: {
      color: theme.palette.text.secondary,
      marginBottom: theme.spacing(1)
    }
  });

interface State {
  result: ErrorsResponse | null;
  error: string | null;
}

class Errors extends React.Component<
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
      const result = await getErrors(hostname, pid);
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
      <DialogWithTitle handleClose={this.handleClose} title="Errors">
        {error !== null ? (
          <Typography color="error">{error}</Typography>
        ) : result === null ? (
          <Typography color="textSecondary">Loading...</Typography>
        ) : (
          Object.entries(result).map(([pid, errors]) => (
            <React.Fragment key={pid}>
              <Typography className={classes.header}>
                {hostname} (PID: {pid})
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
