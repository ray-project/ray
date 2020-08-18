import {
  Button,
  Box,
  createStyles,
  makeStyles,
  MenuItem,
  Select,
  Table,
  TableBody,
  Theme,
  InputLabel, FormControl
} from "@material-ui/core";
import PauseIcon from "@material-ui/icons/Pause";
import PlayArrowIcon from "@material-ui/icons/PlayArrow";
import React, { useState } from "react";
import { useDispatch, useSelector } from "react-redux";
import { useInterval } from "../../../common/reactUtils";
import {
  MemoryTableEntry,
  MemoryTableGroups,
  stopMemoryTableCollection,
  getMemoryTable,
  MemoryGroupByKey,
  MemoryTableResponse
} from "../../../api";
import SortableTableHead, {
  HeaderInfo,
} from "../../../common/SortableTableHead";
import { getComparator, Order, stableSort } from "../../../common/tableUtils";
import { StoreState } from "../../../store";
import { dashboardActions } from "../state";
import ExpanderRow from "./ExpanderRow";
import MemoryRowGroup from "./MemoryRowGroup";
import { MemoryTableRow } from "./MemoryTableRow";

const DEFAULT_ENTRIES_PER_GROUP = 10;
const DEFAULT_UNGROUPED_ENTRIES = 25;

type GroupedMemoryRowsProps = {
  memoryTableGroups: MemoryTableGroups;
  order: Order;
  orderBy: keyof MemoryTableEntry | null;
};

const GroupedMemoryRows: React.FC<GroupedMemoryRowsProps> = ({
  memoryTableGroups,
  order,
  orderBy,
}) => {
  const comparator = orderBy && getComparator(order, orderBy);
  return (
    <React.Fragment>
      {Object.entries(memoryTableGroups).map(([groupKey, group]) => {
        const sortedEntries = comparator
          ? stableSort(group.entries, comparator)
          : group.entries;

        return (
          <MemoryRowGroup
            groupKey={groupKey}
            summary={group.summary}
            entries={sortedEntries}
            initialExpanded={true}
            initialVisibleEntries={DEFAULT_ENTRIES_PER_GROUP}
          />
        );
      })}
    </React.Fragment>
  );
};

type UngroupedMemoryRowsProps = {
  memoryTableGroups: MemoryTableGroups;
  order: Order;
  orderBy: memoryColumnId | null;
};

const UngroupedMemoryRows: React.FC<UngroupedMemoryRowsProps> = ({
  memoryTableGroups,
  order,
  orderBy,
}) => {
  const [visibleEntries, setVisibleEntries] = useState(
    DEFAULT_UNGROUPED_ENTRIES,
  );
  const onExpand = () => setVisibleEntries(visibleEntries + 10);
  const allEntries = Object.values(memoryTableGroups).reduce(
    (allEntries: Array<MemoryTableEntry>, memoryTableGroup) => {
      const groupEntries = memoryTableGroup.entries;
      return allEntries.concat(groupEntries);
    },
    [],
  );
  const sortedEntries =
    orderBy === null
      ? allEntries
      : stableSort(allEntries, getComparator(order, orderBy));
  return (
    <React.Fragment>
      {" "}
      {sortedEntries.slice(0, visibleEntries).map((memoryTableEntry, index) => (
        <MemoryTableRow
          memoryTableEntry={memoryTableEntry}
          key={index.toString()}
        />
      ))}
      <ExpanderRow onExpand={onExpand} />
    </React.Fragment>
  );
};

type memoryColumnId =
  | "node_ip_address"
  | "pid"
  | "type"
  | "object_ref"
  | "object_size"
  | "reference_type"
  | "call_site";

const memoryHeaderInfo: HeaderInfo<memoryColumnId>[] = [
  { id: "node_ip_address", label: "IP Address", numeric: true, sortable: true },
  { id: "pid", label: "pid", numeric: true, sortable: true },
  { id: "type", label: "Type", numeric: false, sortable: true },
  { id: "object_ref", label: "Object Ref", numeric: false, sortable: true },
  {
    id: "object_size",
    label: "Object Size (B)",
    numeric: true,
    sortable: true,
  },
  {
    id: "reference_type",
    label: "Reference Type",
    numeric: false,
    sortable: true,
  },
  { id: "call_site", label: "Call Site", numeric: false, sortable: true },
];

const useMemoryInfoStyles = makeStyles((theme: Theme) =>
  createStyles({
    table: {
      marginTop: theme.spacing(1),
    },
    cell: {
      padding: theme.spacing(1),
      textAlign: "center",
    },
    pauseButton: {
      margin: theme.spacing(1),
      padding: theme.spacing(1),
      float: "right",
    },
  }),
);

const memoryInfoSelector = (state: StoreState) => ({
  tab: state.dashboard.tab,
  memoryTable: state.dashboard.memoryTable,
  shouldObtainMemoryTable: state.dashboard.shouldObtainMemoryTable,
});

const fetchMemoryTable = (groupByKey: MemoryGroupByKey, setResults: (mtr: MemoryTableResponse) => void) => {
  return async () => {
    const resp = await getMemoryTable(groupByKey);
    setResults(resp);
  };
};

const MemoryInfo: React.FC<{}> = () => {
  const { memoryTable } = useSelector(
    memoryInfoSelector,
  );
  const dispatch = useDispatch();
  const [paused, setPaused] = useState(false);
  const pauseButtonIcon = paused ? (
    <PlayArrowIcon />
  ) : (
      <PauseIcon />
    );
  const classes = useMemoryInfoStyles();
  const [groupBy, setGroupBy] = useState<MemoryGroupByKey>("node");
  const toggleOrder = () => setOrder(order === "asc" ? "desc" : "asc");
  const [order, setOrder] = React.useState<Order>("asc");
  const [orderBy, setOrderBy] = React.useState<memoryColumnId | null>(null);

  const stopPolling = useInterval(
    fetchMemoryTable(groupBy, (resp) => dispatch(dashboardActions.setMemoryTable(resp))), 4000);
  return (
    <React.Fragment>
      {memoryTable !== null ? (
        <React.Fragment>
          <FormControl>
            <InputLabel shrink id="group-by-label">Group by</InputLabel>
            <Select
              labelId="group-by-label"
              value={groupBy}
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
              setPaused(true);
              stopPolling();
              stopMemoryTableCollection();
            }}>
            {pauseButtonIcon}
            {paused ? "Resume Collection" : "Pause Collection"}
          </Button>
          <Table className={classes.table}>
            <SortableTableHead
              orderBy={orderBy}
              order={order}
              onRequestSort={(_, property) => {
                if (property === orderBy) {
                  toggleOrder();
                } else {
                  setOrderBy(property);
                  setOrder("asc");
                }
              }}
              headerInfo={memoryHeaderInfo}
              firstColumnEmpty={false}
            />
            <TableBody>
              {groupBy ? (
                <GroupedMemoryRows
                  memoryTableGroups={memoryTable.group}
                  order={order}
                  orderBy={orderBy}
                />
              ) : (
                  <UngroupedMemoryRows
                    memoryTableGroups={memoryTable.group}
                    order={order}
                    orderBy={orderBy}
                  />
                )}
            </TableBody>
          </Table>
        </React.Fragment>
      ) : (
          <div>No Memory Table Information Provided</div>
        )}
    </React.Fragment>
  );
};

export default MemoryInfo;
