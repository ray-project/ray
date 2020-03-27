import {
  createStyles,
  Tab,
  Tabs,
  Theme,
  Typography,
  WithStyles,
  withStyles,
} from "@material-ui/core";
import WarningRoundedIcon from "@material-ui/icons/WarningRounded";
import React from "react";
import { connect } from "react-redux";
import { getTuneInfo } from "../../../api";
import { StoreState } from "../../../store";
import { dashboardActions } from "../state";
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
    warning: {
      fontSize: "0.8125rem",
    },
    warningIcon: {
      fontSize: "1.25em",
      verticalAlign: "text-bottom",
    },
  });

const mapStateToProps = (state: StoreState) => ({
  tuneInfo: state.dashboard.tuneInfo,
});

const mapDispatchToProps = dashboardActions;

type State = {
  tabIndex: number;
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
  };

  refreshTuneInfo = async () => {
    try {
      const tuneInfo = await getTuneInfo();
      this.props.setTuneInfo(tuneInfo);
    } catch (error) {
      this.props.setError(error.toString());
    } finally {
      this.timeout = window.setTimeout(this.refreshTuneInfo, 1000);
    }
  };

  async componentDidMount() {
    await this.refreshTuneInfo();
  }

  async componentWillUnmount() {
    window.clearTimeout(this.timeout);
  }

  handleTabChange = (event: React.ChangeEvent<{}>, value: number) => {
    this.setState({
      tabIndex: value,
    });
  };

  render() {
    const { classes } = this.props;

    const { tabIndex } = this.state;

    const tabs = [
      { label: "Table", component: TuneTable },
      { label: "TensorBoard", component: TuneTensorBoard },
    ];

    const SelectedComponent = tabs[tabIndex].component;
    return (
      <div className={classes.root}>
        <Typography className={classes.warning} color="textSecondary">
          <WarningRoundedIcon className={classes.warningIcon} /> Note: This tab
          is experimental.
        </Typography>
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
