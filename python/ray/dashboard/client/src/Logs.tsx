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
      right: theme.spacing(1),
      top: theme.spacing(1),
      zIndex: 1
    },
    title: {
      borderBottomColor: theme.palette.divider,
      borderBottomStyle: "solid",
      borderBottomWidth: 1,
      lineHeight: 1,
      marginBottom: theme.spacing(3),
      paddingBottom: theme.spacing(3),
      position: "relative",
      "&:not(:first-of-type)": {
        marginTop: theme.spacing(3)
      }
    }
  });

interface Props {
  ipToHostname: {
    [ip: string]: string;
  };
  logs: {
    [ip: string]: {
      [pid: string]: string[];
    };
  };
}

class Component extends React.Component<
  Props &
    WithStyles<typeof styles> &
    RouteComponentProps<{ hostname: string; pid: string | undefined }>
> {
  handleClose = () => {
    this.props.history.push("/");
  };

  render() {
    const { classes, ipToHostname, logs, match } = this.props;
    const { hostname, pid } = match.params;

    let logsForHost: {
      [pid: string]: string[];
    } = {};

    for (const ip of Object.keys(ipToHostname)) {
      if (ipToHostname[ip] === hostname) {
        if (ip in logs) {
          logsForHost = logs[ip];
        }
        break;
      }
    }

    const logsToDisplay =
      pid === undefined
        ? logsForHost
        : { [pid]: pid in logsForHost ? logsForHost[pid] : [] };

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
        {Object.entries(logsToDisplay).map(([pid, lines]) => (
          <React.Fragment key={pid}>
            <Typography className={classes.title}>
              {hostname} (PID: {pid})
            </Typography>
            {lines.length > 0 ? (
              <NumberedLines lines={lines} />
            ) : (
              <Typography color="textSecondary">No logs found.</Typography>
            )}
          </React.Fragment>
        ))}
      </Dialog>
    );
  }
}

export default withStyles(styles)(Component);
