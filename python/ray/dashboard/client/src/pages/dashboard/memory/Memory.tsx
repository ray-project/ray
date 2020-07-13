import {
  Button,
  Checkbox,
  createStyles,
  FormControlLabel,
  makeStyles,
  Table,
  TableBody,
  Theme,
} from "@material-ui/core";
import PauseIcon from "@material-ui/icons/Pause";
import PlayArrowIcon from "@material-ui/icons/PlayArrow";
import React, { useState } from "react";
import { useDispatch, useSelector } from "react-redux";
import {
  MemoryTableEntry,
  MemoryTableGroups,
  stopMemoryTableCollection,
} from "../../../api";
import SortableTableHead, {
  HeaderInfo,
} from "../../../common/SortableTableHead";
import { getComparator, Order, stableSort } from "../../../common/tableUtils";
import { StoreState } from "../../../store";
import { dashboardActions } from "../state";
import MemoryRowGroup from "./MemoryRowGroup";
import { MemoryTableRow } from "./MemoryTableRow";

const makeGroupedEntries = (
  memoryTableGroups: MemoryTableGroups,
  order: Order,
  orderBy: keyof MemoryTableEntry | null,
) => {
  const comparator = orderBy && getComparator(order, orderBy);
  return Object.entries(memoryTableGroups).map(([groupKey, group]) => {
    const sortedEntries = comparator
      ? stableSort(group.entries, comparator)
      : group.entries;

    return (
      <MemoryRowGroup
        groupKey={groupKey}
        summary={group.summary}
        entries={sortedEntries}
        initialExpanded={true}
      />
    );
  });
};

const makeUngroupedEntries = (
  memoryTableGroups: MemoryTableGroups,
  order: Order,
  orderBy: memoryColumnId | null,
) => {
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
  return sortedEntries.map((memoryTableEntry, index) => (
    <MemoryTableRow
      memoryTableEntry={memoryTableEntry}
      key={`mem-row-${index}`}
    />
  ));
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
  }),
);

const memoryInfoSelector = (state: StoreState) => ({
  tab: state.dashboard.tab,
  memoryTable: state.dashboard.memoryTable,
  shouldObtainMemoryTable: state.dashboard.shouldObtainMemoryTable,
});

const MemoryInfo: React.FC<{}> = () => {
  const { memoryTable, shouldObtainMemoryTable } = useSelector(
    memoryInfoSelector,
  );
  const dispatch = useDispatch();
  const toggleMemoryCollection = async () => {
    dispatch(
      dashboardActions.setShouldObtainMemoryTable(!shouldObtainMemoryTable),
    );
    if (shouldObtainMemoryTable) {
      await stopMemoryTableCollection();
    }
  };

  const pauseButtonIcon = shouldObtainMemoryTable ? (
    <PauseIcon />
  ) : (
    <PlayArrowIcon />
  );
  const classes = useMemoryInfoStyles();
  const [isGrouped, setIsGrouped] = useState(true);
  const [order, setOrder] = React.useState<Order>("asc");
  const toggleOrder = () => setOrder(order === "asc" ? "desc" : "asc");
  const [orderBy, setOrderBy] = React.useState<memoryColumnId | null>(null);
  return (
    <React.Fragment>
      {memoryTable !== null ? (
        <React.Fragment>
          <Button color="primary" onClick={toggleMemoryCollection}>
            {pauseButtonIcon}
            {shouldObtainMemoryTable ? "Pause Collection" : "Resume Collection"}
          </Button>
          <FormControlLabel
            control={
              <Checkbox
                checked={isGrouped}
                onChange={() => setIsGrouped(!isGrouped)}
                color="primary"
              />
            }
            label="Group by host"
          />
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
              {isGrouped
                ? makeGroupedEntries(memoryTable.group, order, orderBy)
                : makeUngroupedEntries(memoryTable.group, order, orderBy)}
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
