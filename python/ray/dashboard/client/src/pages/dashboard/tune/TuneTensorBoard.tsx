import {
  createStyles,
  Theme,
  Typography,
  WithStyles,
  withStyles,
} from "@material-ui/core";
import React from "react";
import { connect } from "react-redux";
import { StoreState } from "../../../store";
import { dashboardActions } from "../state";

const styles = (theme: Theme) =>
  createStyles({
    root: {
      backgroundColor: theme.palette.background.paper,
      padding: theme.spacing(2),
      "& > :not(:first-child)": {
        marginTop: theme.spacing(4),
      },
    },
    board: {
      width: "100%",
      height: "1000px",
      border: "none",
    },
    warning: {
      fontSize: "0.8125rem",
    },
  });

const mapStateToProps = (state: StoreState) => ({
  error: state.dashboard.error,
});

const mapDispatchToProps = dashboardActions;

class TuneTensorBoard extends React.Component<
  WithStyles<typeof styles> &
    ReturnType<typeof mapStateToProps> &
    typeof mapDispatchToProps
> {
  render() {
    const { classes, error } = this.props;

    return (
      <div className={classes.root}>
        {error === "TypeError: Failed to fetch" && (
          <Typography className={classes.warning} color="textSecondary">
            Warning: Tensorboard server closed. View Tensorboard by running
            "tensorboard --logdir" if not displaying below.
          </Typography>
        )}
        <iframe
          src="http://localhost:6006/"
          className={classes.board}
          title="TensorBoard"
        ></iframe>
      </div>
    );
  }
}

export default connect(
  mapStateToProps,
  mapDispatchToProps,
)(withStyles(styles)(TuneTensorBoard));
