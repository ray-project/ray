import {
  Box,
  Button,
  CircularProgress,
  createStyles,
  FormControl,
  InputLabel,
  makeStyles,
  MenuItem,
  Select,
  Theme,
  Typography,
} from "@material-ui/core";
import PauseIcon from "@material-ui/icons/Pause";
import PlayArrowIcon from "@material-ui/icons/PlayArrow";
import SubdirectoryArrowRightIcon from "@material-ui/icons/SubdirectoryArrowRight";
import React, { useCallback, useEffect, useRef, useState } from "react";
import { useDispatch, useSelector } from "react-redux";
import {
  getMemoryTable,
  MemoryGroupByKey,
  MemoryTable,
  MemoryTableResponse,
  setMemoryTableCollection,
} from "../../../api";
import { StoreState } from "../../../store";
import { dashboardActions } from "../state";
import MemoryRowGroup from "./MemoryRowGroup";

const groupTitle = (groupKey: string, groupBy: MemoryGroupByKey) => {
  if (groupBy === "node") {
    return <Typography variant="h6">{`Node ${groupKey}`}</Typography>;
  }
  if (groupBy === "stack_trace") {
    return <PyStackTrace stackTrace={groupKey} />;
  }
  if (groupBy === "") {
    return <Typography variant="h6">All entries</Typography>;
  }
  return <Typography variant="h6">Unknown Group</Typography>;
};

const PyStackTrace: React.FC<{ stackTrace: string }> = ({ stackTrace }) => {
  const stackFrames = stackTrace.split(" | ");
  const renderedFrames = stackFrames.map((frame, i) => (
    <Typography
      variant={i === 0 ? "h6" : "subtitle2"}
      style={{ marginLeft: `${i}em` }}
      key={i}
    >
      {i !== 0 && <SubdirectoryArrowRightIcon />}
      {frame}
    </Typography>
  ));
  return <Box>{renderedFrames}</Box>;
};

const MEMORY_POLLING_INTERVAL_MS = 4000;

const useMemoryInfoStyles = makeStyles((theme: Theme) =>
  createStyles({
    pauseButton: {
      margin: theme.spacing(1),
      padding: theme.spacing(1),
      float: "right",
    },
    select: {
      minWidth: "7em",
    },
  }),
);

const memoryTableSelector = (state: StoreState) => state.dashboard.memoryTable;
const isEmpty = (memoryTable: MemoryTable) =>
  Object.keys(memoryTable.group).length === 0;
const loadTimerComplete = (mountedAt: Date) => {
  const secondsBetween = Math.abs(
    (new Date().getTime() - mountedAt.getTime()) / 1000,
  );
  return secondsBetween > 10;
};

const fetchMemoryTable = (
  groupByKey: MemoryGroupByKey,
  setResults: (mtr: MemoryTableResponse) => void,
) => {
  return async () => {
    const resp = await getMemoryTable(groupByKey);
    setResults(resp);
  };
};

const MemoryInfo: React.FC<{}> = () => {
  const memoryTable = useSelector(memoryTableSelector);
  const dispatch = useDispatch();
  const mountedAt = new Date();
  const [paused, setPaused] = useState(false);
  const pauseButtonIcon = paused ? <PlayArrowIcon /> : <PauseIcon />;

  const classes = useMemoryInfoStyles();
  const [groupBy, setGroupBy] = useState<MemoryGroupByKey>("node");

  // Turn memory collection on render
  useEffect(() => {
    setMemoryTableCollection(true);
    return () => {
      setMemoryTableCollection(false);
    };
  }, []);

  // Set up polling memory data
  const fetchData = useCallback(
    fetchMemoryTable(groupBy, (resp) =>
      dispatch(dashboardActions.setMemoryTable(resp)),
    ),
    [groupBy],
  );
  const intervalId = useRef<any>(null);
  useEffect(() => {
    if (!intervalId.current && !paused) {
      fetchData();
      intervalId.current = setInterval(fetchData, MEMORY_POLLING_INTERVAL_MS);
    }
    const cleanup = () => {
      if (intervalId.current) {
        clearInterval(intervalId.current);
        intervalId.current = null;
      }
    };
    return cleanup;
  }, [paused, fetchData, dispatch]);

  if (!memoryTable || (isEmpty(memoryTable) && !loadTimerComplete(mountedAt))) {
    return (
      <Typography align="center" color="textSecondary" variant="h3">
        <CircularProgress /> Loading
      </Typography>
    );
  }
  if (isEmpty(memoryTable) && loadTimerComplete(mountedAt)) {
    return (
      <Typography align="center" color="textSecondary" variant="h3">
        Finished loading, but nothing found.
      </Typography>
    );
  }

  const children = Object.entries(memoryTable.group)
    .sort(([key1], [key2]) => (key1 < key2 ? -1 : 1))
    .map(([groupKey, memoryGroup]) => (
      <MemoryRowGroup
        key={groupKey}
        groupKey={groupKey}
        groupTitle={groupTitle(groupKey, groupBy)}
        entries={memoryGroup.entries}
        summary={memoryGroup.summary}
        initialExpanded={false}
        initialVisibleEntries={10}
      />
    ));
  return (
    <Box>
      <FormControl>
        <InputLabel shrink id="group-by-label">
          Group by
        </InputLabel>
        <Select
          labelId="group-by-label"
          value={groupBy}
          className={classes.select}
          onChange={(e: any) => setGroupBy(e.target.value)}
          color="primary"
          displayEmpty
        >
          <MenuItem value="">
            <em>None</em>
          </MenuItem>
          <MenuItem value={"node"}>Node IP Address</MenuItem>
          <MenuItem value={"stack_trace"}>Stack Trace</MenuItem>
        </Select>
      </FormControl>
      <Button
        color="primary"
        className={classes.pauseButton}
        onClick={() => {
          if (intervalId.current) {
            clearInterval(intervalId.current);
            intervalId.current = null;
          }
          setMemoryTableCollection(!paused);
          setPaused(!paused);
        }}
      >
        {pauseButtonIcon}
        {paused ? "Resume Collection" : "Pause Collection"}
      </Button>
      {children}
    </Box>
  );
};

export default MemoryInfo;
