import {
  Button,
  createStyles,
  makeStyles,
  Tab,
  Tabs,
  Theme,
  Typography,
} from "@material-ui/core";
import React, { useCallback, useEffect, useRef } from "react";
import { useDispatch, useSelector } from "react-redux";
import { useHistory } from "react-router-dom";
import { getActorGroups, getNodeInfo, getTuneAvailability } from "../../api";
import { StoreState } from "../../store";
import LastUpdated from "./LastUpdated";
import LogicalView from "./logical-view/LogicalView";
import MemoryInfo from "./memory/Memory";
import NodeInfo from "./node-info/NodeInfo";
import RayConfig from "./ray-config/RayConfig";
import { dashboardActions } from "./state";
import Tune from "./tune/Tune";

const {
  setNodeInfo,
  setTuneAvailability,
  setActorGroups,
  setError,
  setTab,
} = dashboardActions;
const useDashboardStyles = makeStyles((theme: Theme) =>
  createStyles({
    root: {
      backgroundColor: theme.palette.background.paper,
      padding: theme.spacing(2),
      "& > :not(:first-child)": {
        marginTop: theme.spacing(4),
      },
      position: "relative",
    },
    tabs: {
      borderBottomColor: theme.palette.divider,
      borderBottomStyle: "solid",
      borderBottomWidth: 1,
    },
  }),
);

const tabSelector = (state: StoreState) => state.dashboard.tab;
const tuneAvailabilitySelector = (state: StoreState) =>
  state.dashboard.tuneAvailability;

const allTabs = [
  { label: "Machine view", component: NodeInfo },
  { label: "Logical view", component: LogicalView },
  { label: "Memory", component: MemoryInfo },
  { label: "Ray config", component: RayConfig },
  { label: "Tune", component: Tune },
];

const Dashboard: React.FC = () => {
  const dispatch = useDispatch();
  const tuneAvailability = useSelector(tuneAvailabilitySelector);
  const tab = useSelector(tabSelector);
  const classes = useDashboardStyles();
  const history = useHistory();

  // Polling Function
  const refreshInfo = useCallback(async () => {
    try {
      const [nodeInfo, tuneAvailability, actorGroups] = await Promise.all([
        getNodeInfo(),
        getTuneAvailability(),
        getActorGroups(),
      ]);
      dispatch(setNodeInfo({ nodeInfo }));
      dispatch(setTuneAvailability(tuneAvailability));
      dispatch(setActorGroups(actorGroups));
      dispatch(setError(null));
    } catch (error) {
      dispatch(setError(error.toString()));
    }
  }, [dispatch]);

  // Run the poller
  const intervalId = useRef<any>(null);
  useEffect(() => {
    if (intervalId.current === null) {
      refreshInfo();
      intervalId.current = setInterval(refreshInfo, 1000);
    }
    const cleanup = () => {
      clearInterval(intervalId.current);
    };
    return cleanup;
  }, [refreshInfo]);

  const handleTabChange = (_: any, value: number) => dispatch(setTab(value));

  const tabs = allTabs.slice();

  // if Tune information is not available, remove Tune tab from the dashboard
  if (tuneAvailability === null || !tuneAvailability.available) {
    tabs.splice(4);
  }

  const SelectedComponent = tabs[tab].component;
  return (
    <div className={classes.root}>
      <Typography variant="h5">Ray Dashboard</Typography>
      <Button
        style={{ position: "absolute", right: 16, top: 16 }}
        variant="contained"
        size="small"
        color="primary"
        onClick={() => history.push("/node")}
      >
        Try Experimental Dashboard
      </Button>
      <Tabs
        className={classes.tabs}
        indicatorColor="primary"
        onChange={handleTabChange}
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
};

export default Dashboard;
