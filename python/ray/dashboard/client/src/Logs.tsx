import Dialog from "@material-ui/core/Dialog";
import IconButton from "@material-ui/core/IconButton";
import { Theme } from "@material-ui/core/styles/createMuiTheme";
import createStyles from "@material-ui/core/styles/createStyles";
import withStyles, { WithStyles } from "@material-ui/core/styles/withStyles";
import Typography from "@material-ui/core/Typography";
import CloseIcon from "@material-ui/icons/Close";
import React from "react";
import { RouteComponentProps } from "react-router";
import NumberedLines from "./NumberedLines";

const styles = (theme: Theme) =>
  createStyles({
    paper: {
      padding: theme.spacing(3)
    },
    closeButton: {
      position: "absolute",
      right: theme.spacing(1.5),
      top: theme.spacing(1.5),
      zIndex: 1
    },
    title: {
      borderBottomColor: theme.palette.divider,
      borderBottomStyle: "solid",
      borderBottomWidth: 1,
      fontSize: "1.5rem",
      lineHeight: 1,
      marginBottom: theme.spacing(3),
      paddingBottom: theme.spacing(3)
    },
    header: {
      lineHeight: 1,
      marginBottom: theme.spacing(3),
      marginTop: theme.spacing(3)
    }
  });

interface State {
  result: { [pid: string]: string[] } | null;
  error: string | null;
}

class Component extends React.Component<
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
      const url = new URL(
        "/api/logs",
        process.env.NODE_ENV === "development"
          ? "http://localhost:8080"
          : window.location.origin
      );
      url.searchParams.set("hostname", hostname);
      url.searchParams.set("pid", pid || "");
      const response = await fetch(url.toString());
      const json = await response.json();
      this.setState({ result: json.result, error: null });
    } catch (error) {
      this.setState({ result: null, error: error.toString() });
    }
  }

  render() {
    const { classes, match } = this.props;
    const { result, error } = this.state;

    const { hostname } = match.params;

    return (
      <Dialog
        classes={{ paper: classes.paper }}
        fullWidth
        maxWidth="md"
        onClose={this.handleClose}
        open
        scroll="body"
      >
        <IconButton className={classes.closeButton} onClick={this.handleClose}>
          <CloseIcon />
        </IconButton>
        <Typography className={classes.title}>Logs</Typography>
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
                <NumberedLines lines={lines} />
              ) : (
                <Typography color="textSecondary">No logs found.</Typography>
              )}
            </React.Fragment>
          ))
        )}
      </Dialog>
    );
  }
}

export default withStyles(styles)(Component);
