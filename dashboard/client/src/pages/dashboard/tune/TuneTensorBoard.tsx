import {
  Button,
  CircularProgress,
  createStyles,
  Theme,
  Typography,
  withStyles,
  WithStyles,
} from "@material-ui/core";
import React from "react";
import { connect } from "react-redux";
import { enableTuneTensorBoard } from "../../../api";
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
    progress: {
      marginLeft: "10px",
      marginTop: "2px",
    },
  });

const mapStateToProps = (state: StoreState) => ({
  error: state.dashboard.error,
  tuneInfo: state.dashboard.tuneInfo,
});

type State = {
  tensorBoardEnabled: boolean;
  loading: boolean;
};

const mapDispatchToProps = dashboardActions;

class TuneTensorBoard extends React.Component<
  WithStyles<typeof styles> &
    ReturnType<typeof mapStateToProps> &
    typeof mapDispatchToProps,
  State
> {
  state: State = {
    tensorBoardEnabled: false,
    loading: false,
  };

  enableTensorBoard() {
    enableTuneTensorBoard();
    this.setState({
      tensorBoardEnabled: true,
    });
  }

  handleSubmit = () => {
    this.setState({ loading: true });
    enableTuneTensorBoard().then(() => {
      this.setState({ loading: false });
    });
  };

  tensorBoard = () => {
    const { classes, error, tuneInfo } = this.props;

    return (
      <div>
        {error === "TypeError: Failed to fetch" && (
          <Typography className={classes.warning} color="textSecondary">
            Warning: Tensorboard server closed. View Tensorboard by running
            "tensorboard --logdir" if not displaying below.
          </Typography>
        )}
        {tuneInfo && !tuneInfo.tensorboard.tensorboardCurrent && (
          <Typography className={classes.warning} color="textSecondary">
            The below Tensorboard reflects a previously entered log directory.
            Restart the Ray Dashboard to change the Tensorboard logdir.
          </Typography>
        )}
        <iframe
          src="http://localhost:6006/"
          className={classes.board}
          title="TensorBoard"
        ></iframe>
      </div>
    );
  };

  render() {
    const { classes, tuneInfo } = this.props;

    const { loading } = this.state;

    if (tuneInfo === null) {
      return;
    }
    const enabled = tuneInfo.tensorboard.tensorboardEnabled;
    return (
      <div className={classes.root}>
        {!enabled && (
          <div>
            <Button
              variant="outlined"
              onClick={this.handleSubmit}
              className={classes.warning}
            >
              Enable TensorBoard
            </Button>
            {loading && (
              <CircularProgress className={classes.progress} size={25} />
            )}
          </div>
        )}

        {enabled && this.tensorBoard()}
      </div>
    );
  }
}

export default connect(
  mapStateToProps,
  mapDispatchToProps,
)(withStyles(styles)(TuneTensorBoard));
