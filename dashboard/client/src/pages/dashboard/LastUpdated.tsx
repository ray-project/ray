import {
  createStyles,
  Theme,
  Typography,
  WithStyles,
  withStyles,
} from "@material-ui/core";
import React from "react";
import { connect } from "react-redux";
import { StoreState } from "../../store";

const styles = (theme: Theme) =>
  createStyles({
    root: {
      marginTop: theme.spacing(2),
    },
    lastUpdated: {
      color: theme.palette.text.secondary,
      fontSize: "0.8125rem",
      textAlign: "center",
    },
    error: {
      color: theme.palette.error.main,
      fontSize: "0.8125rem",
      textAlign: "center",
    },
  });

const mapStateToProps = (state: StoreState) => ({
  lastUpdatedAt: state.dashboard.lastUpdatedAt,
  error: state.dashboard.error,
});

class LastUpdated extends React.Component<
  WithStyles<typeof styles> & ReturnType<typeof mapStateToProps>
> {
  render() {
    const { classes, lastUpdatedAt, error } = this.props;
    return (
      <div className={classes.root}>
        {lastUpdatedAt !== null && (
          <Typography className={classes.lastUpdated}>
            Last updated: {new Date(lastUpdatedAt).toLocaleString()}
          </Typography>
        )}
        {error !== null && (
          <Typography className={classes.error}>{error}</Typography>
        )}
      </div>
    );
  }
}

export default connect(mapStateToProps)(withStyles(styles)(LastUpdated));
