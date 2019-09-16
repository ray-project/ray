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
        marginTop: theme.spacing(6)
      }
    },
    error: {
      "&:not(:last-child)": {
        marginBottom: theme.spacing(3)
      }
    },
    timestamp: {
      marginBottom: theme.spacing(1)
    }
  });

interface Props {
  errors: {
    [jobId: string]: Array<{
      message: string;
      timestamp: number;
      type: string;
    }>;
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
    const { classes, errors, match } = this.props;
    const { hostname, pid } = match.params;

    let errorsForHost: {
      [pid: string]: Array<{
        lines: string[];
        timestamp: number;
      }>;
    } = {};

    for (const jobErrors of Object.values(errors)) {
      for (const error of jobErrors) {
        const match = error.message.match(/\(pid=(\d+), host=(.*?)\)/);
        if (match !== null && match[2] === hostname) {
          const pid = match[1];
          if (!(pid in errorsForHost)) {
            errorsForHost[pid] = [];
          }
          errorsForHost[pid].push({
            lines: error.message
              .replace(/\u001b\[\d+m/g, "") // eslint-disable-line no-control-regex
              .trim()
              .split("\n"),
            timestamp: error.timestamp
          });
        }
      }
    }

    const errorsToDisplay =
      pid === undefined
        ? errorsForHost
        : { [pid]: pid in errorsForHost ? errorsForHost[pid] : [] };

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
        {Object.entries(errorsToDisplay).map(([pid, errors]) => (
          <React.Fragment key={pid}>
            <Typography className={classes.title}>
              {hostname} (PID: {pid})
            </Typography>
            {errors.length > 0 ? (
              errors.map(({ lines, timestamp }, index) => (
                <div className={classes.error} key={index}>
                  <Typography className={classes.timestamp}>
                    Error at {new Date(timestamp * 1000).toLocaleString()}
                  </Typography>
                  <NumberedLines lines={lines} />
                </div>
              ))
            ) : (
              <Typography color="textSecondary">No errors found.</Typography>
            )}
          </React.Fragment>
        ))}
      </Dialog>
    );
  }
}

export default withStyles(styles)(Component);
