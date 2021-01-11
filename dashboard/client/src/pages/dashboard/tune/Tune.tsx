import {
  Button,
  CircularProgress,
  createStyles,
  Tab,
  Tabs,
  TextField,
  Theme,
  Typography,
  WithStyles,
  withStyles,
} from "@material-ui/core";
import React from "react";
import { connect } from "react-redux";
import { getTuneInfo, setTuneExperiment } from "../../../api";
import { StoreState } from "../../../store";
import { dashboardActions } from "../state";
import TuneErrors from "./TuneErrors";
import TuneTable from "./TuneTable";
import TuneTensorBoard from "./TuneTensorBoard";

const styles = (theme: Theme) =>
  createStyles({
    root: {
      backgroundColor: theme.palette.background.paper,
    },
    tabs: {
      borderBottomColor: theme.palette.divider,
      borderBottomStyle: "solid",
      borderBottomWidth: 1,
    },
    heading: {
      fontsize: "0.9em",
      marginTop: theme.spacing(2),
    },
    warning: {
      fontSize: "1em",
    },
    warningIcon: {
      fontSize: "1.25em",
      verticalAlign: "text-bottom",
    },
    formControl: {
      margin: theme.spacing(1),
      minWidth: 120,
    },
    submit: {
      marginLeft: theme.spacing(2),
      fontSize: "0.8125em",
    },
    prompt: {
      fontSize: "1em",
      marginTop: theme.spacing(1),
    },
    input: {
      width: "85%",
    },
    progress: {
      marginLeft: theme.spacing(2),
    },
  });

const mapStateToProps = (state: StoreState) => ({
  tuneInfo: state.dashboard.tuneInfo,
  tuneAvailability: state.dashboard.tuneAvailability,
});

const mapDispatchToProps = dashboardActions;

type State = {
  tabIndex: number;
  experiment: string;
  loading: boolean;
};

class Tune extends React.Component<
  WithStyles<typeof styles> &
    ReturnType<typeof mapStateToProps> &
    typeof mapDispatchToProps,
  State
> {
  timeout: number = 0;

  state: State = {
    tabIndex: 0,
    experiment: "",
    loading: false,
  };

  refreshTuneInfo = async () => {
    try {
      if (
        this.props.tuneAvailability &&
        this.props.tuneAvailability.available
      ) {
        const tuneInfo = await getTuneInfo();
        this.props.setTuneInfo(tuneInfo);
      }
    } catch (error) {
      this.props.setError(error.toString());
    } finally {
      this.timeout = window.setTimeout(this.refreshTuneInfo, 1000);
    }
  };

  async componentWillUnmount() {
    window.clearTimeout(this.timeout);
  }

  handleTabChange = (event: React.ChangeEvent<{}>, value: number) => {
    this.setState({
      tabIndex: value,
    });
  };

  handleExperimentChange = (event: React.ChangeEvent<{ value: any }>) => {
    this.setState({
      experiment: event.target.value,
    });
  };

  handleExperimentSubmit = async () => {
    this.setState({ loading: true });
    try {
      await setTuneExperiment(this.state.experiment);
      window.clearTimeout(this.timeout);
      await this.refreshTuneInfo();
      this.setState({ loading: false });
    } catch (error) {
      this.props.setError(error.toString());
      this.setState({ loading: false });
    }
  };

  experimentChoice = (prompt: boolean) => {
    const { classes } = this.props;

    const { loading } = this.state;
    return (
      <div>
        {prompt && (
          <Typography className={classes.heading} color="textPrimary">
            You can use this tab to monitor Tune jobs, their statuses,
            hyperparameters, and more. For more information, read the
            documentation{" "}
            <a href="https://docs.ray.io/en/master/ray-dashboard.html#tune">
              here
            </a>
            .
          </Typography>
        )}
        <div>
          <Typography className={classes.prompt} color="textSecondary">
            Enter Tune Log Directory Here:
          </Typography>
          <TextField
            className={classes.input}
            id="standard-basic"
            value={this.state.experiment}
            onChange={this.handleExperimentChange}
          />
          <Button
            className={classes.submit}
            variant="outlined"
            onClick={this.handleExperimentSubmit}
          >
            Submit
          </Button>
          {loading && (
            <CircularProgress className={classes.progress} size={25} />
          )}
        </div>
      </div>
    );
  };

  render() {
    const { classes, tuneInfo, tuneAvailability } = this.props;

    if (tuneAvailability && !tuneAvailability.trialsAvailable) {
      return this.experimentChoice(true);
    }

    const { tabIndex } = this.state;

    const tabs = [
      { label: "Table", component: TuneTable },
      { label: "TensorBoard", component: TuneTensorBoard },
    ];

    if (tuneInfo !== null && Object.keys(tuneInfo.errors).length > 0) {
      tabs.push({ label: "Errors", component: TuneErrors });
    }

    const SelectedComponent = tabs[tabIndex].component;
    return (
      <div className={classes.root}>
        {this.experimentChoice(false)}
        <Tabs
          className={classes.tabs}
          indicatorColor="primary"
          onChange={this.handleTabChange}
          textColor="primary"
          value={tabIndex}
        >
          {tabs.map(({ label }) => (
            <Tab key={label} label={label} />
          ))}
        </Tabs>
        <SelectedComponent />
      </div>
    );
  }
}

export default connect(
  mapStateToProps,
  mapDispatchToProps,
)(withStyles(styles)(Tune));
