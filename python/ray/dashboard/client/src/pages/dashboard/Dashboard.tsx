import { Theme } from "@material-ui/core/styles/createMuiTheme";
import createStyles from "@material-ui/core/styles/createStyles";
import withStyles, { WithStyles } from "@material-ui/core/styles/withStyles";
import Tab from "@material-ui/core/Tab";
import Tabs from "@material-ui/core/Tabs";
import Typography from "@material-ui/core/Typography";
import React from "react";
import { connect } from "react-redux";
import { getNodeInfo, getRayletInfo } from "../../api";
import { StoreState } from "../../store";
import LastUpdated from "./LastUpdated";
import LogicalView from "./logical-view/LogicalView";
import NodeInfo from "./node-info/NodeInfo";
import RayConfig from "./ray-config/RayConfig";
import { dashboardActions } from "./state";

const styles = (theme: Theme) =>
  createStyles({
    root: {
      backgroundColor: theme.palette.background.paper,
      padding: theme.spacing(2),
      "& > :not(:first-child)": {
        marginTop: theme.spacing(4)
      }
    },
    tabs: {
      borderBottomColor: theme.palette.divider,
      borderBottomStyle: "solid",
      borderBottomWidth: 1
    }
  });

const mapStateToProps = (state: StoreState) => ({
  tab: state.dashboard.tab
});

const mapDispatchToProps = dashboardActions;

class Dashboard extends React.Component<
  WithStyles<typeof styles> &
    ReturnType<typeof mapStateToProps> &
    typeof mapDispatchToProps
> {
  refreshNodeAndRayletInfo = async () => {
    try {
      const [nodeInfo, rayletInfo] = await Promise.all([
        getNodeInfo(),
        getRayletInfo()
      ]);
      this.props.setNodeAndRayletInfo({ nodeInfo, rayletInfo });
      this.props.setError(null);
    } catch (error) {
      this.props.setError(error.toString());
    } finally {
      setTimeout(this.refreshNodeAndRayletInfo, 1000);
    }
  };

  async componentDidMount() {
    await this.refreshNodeAndRayletInfo();
  }

  handleTabChange = (event: React.ChangeEvent<{}>, value: number) => {
    this.props.setTab(value);
  };

  render() {
    const { classes, tab } = this.props;
    const tabs = [
      { label: "Machine view", component: NodeInfo },
      { label: "Logical view", component: LogicalView },
      { label: "Ray config", component: RayConfig }
    ];
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
  mapDispatchToProps
)(withStyles(styles)(Dashboard));
