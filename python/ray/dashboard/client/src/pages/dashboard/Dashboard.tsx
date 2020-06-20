import {
  createStyles,
  Tab,
  Tabs,
  Theme,
  Typography,
  WithStyles,
  withStyles,
} from "@material-ui/core";
import React from "react";
import { connect } from "react-redux";
import {
  getMemoryTable,
  getNodeInfo,
  getRayletInfo,
  getTuneAvailability,
  stopMemoryTableCollection,
} from "../../api";
import { StoreState } from "../../store";
import LastUpdated from "./LastUpdated";
import LogicalView from "./logical-view/LogicalView";
import MemoryInfo from "./memory/Memory";
import NodeInfo from "./node-info/NodeInfo";
import RayConfig from "./ray-config/RayConfig";
import { dashboardActions } from "./state";
import Tune from "./tune/Tune";

const styles = (theme: Theme) =>
  createStyles({
    root: {
      backgroundColor: theme.palette.background.paper,
      padding: theme.spacing(2),
      "& > :not(:first-child)": {
        marginTop: theme.spacing(4),
      },
    },
    tabs: {
      borderBottomColor: theme.palette.divider,
      borderBottomStyle: "solid",
      borderBottomWidth: 1,
    },
  });

const mapStateToProps = (state: StoreState) => ({
  tab: state.dashboard.tab,
  tuneAvailability: state.dashboard.tuneAvailability,
  shouldObtainMemoryTable: state.dashboard.shouldObtainMemoryTable,
});

const mapDispatchToProps = dashboardActions;

class Dashboard extends React.Component<
  WithStyles<typeof styles> &
    ReturnType<typeof mapStateToProps> &
    typeof mapDispatchToProps
> {
  timeoutId = 0;
  tabs = [
    { label: "Machine view", component: NodeInfo },
    { label: "Logical view", component: LogicalView },
    { label: "Memory", component: MemoryInfo },
    { label: "Ray config", component: RayConfig },
    { label: "Tune", component: Tune },
  ];

  refreshInfo = async () => {
    const { shouldObtainMemoryTable } = this.props;
    try {
      const [
        nodeInfo,
        rayletInfo,
        memoryTable,
        tuneAvailability,
      ] = await Promise.all([
        getNodeInfo(),
        getRayletInfo(),
        getMemoryTable(shouldObtainMemoryTable),
        getTuneAvailability(),
      ]);
      this.props.setNodeAndRayletInfo({ nodeInfo, rayletInfo });
      this.props.setTuneAvailability(tuneAvailability);
      this.props.setError(null);
      if (shouldObtainMemoryTable) {
        this.props.setMemoryTable(memoryTable);
      }
    } catch (error) {
      this.props.setError(error.toString());
    } finally {
      this.timeoutId = window.setTimeout(this.refreshInfo, 1000);
    }
  };

  async componentDidMount() {
    await this.refreshInfo();
  }

  componentWillUnmount() {
    clearTimeout(this.timeoutId);
  }

  handleTabChange = async (event: React.ChangeEvent<{}>, value: number) => {
    this.props.setTab(value);
    if (this.tabs[value].label === "Memory") {
      this.props.setShouldObtainMemoryTable(true);
    } else {
      this.props.setShouldObtainMemoryTable(false);
      await stopMemoryTableCollection();
    }
  };

  render() {
    const { classes, tab, tuneAvailability } = this.props;
    const tabs = this.tabs.slice();

    // if Tune information is not available, remove Tune tab from the dashboard
    if (tuneAvailability === null || !tuneAvailability.available) {
      tabs.splice(4);
    }

    const SelectedComponent = tabs[tab].component;
    return (
      <div className={classes.root}>
        <Typography variant="h5">Ray Dashboard</Typography>
        <Tabs
          className={classes.tabs}
          indicatorColor="primary"
          onChange={this.handleTabChange}
          textColor="primary"
          value={tab}
        >
          {tabs.map(({ label }) => (
            <Tab key={label} label={label} />
          ))}
        </Tabs>
        <SelectedComponent />
        <LastUpdated />
      </div>
    );
  }
}

export default connect(
  mapStateToProps,
  mapDispatchToProps,
)(withStyles(styles)(Dashboard));
