import {
  Button,
  createStyles,
  makeStyles,
  Table,
  TableBody,
  TableHead,
  TableRow,
  TableSortLabel,
  Theme,
  WithStyles,
  withStyles,
  FormControlLabel,
  Checkbox
} from "@material-ui/core";
import PauseIcon from "@material-ui/icons/Pause";
import PlayArrowIcon from "@material-ui/icons/PlayArrow";
import React, { ReactNode, useState } from "react";
import { connect } from "react-redux";
import { stopMemoryTableCollection, MemoryTableEntry, MemoryTableGroups } from "../../../api";
import { StoreState } from "../../../store";
import { dashboardActions } from "../state";
import MemoryRowGroup from "./MemoryRowGroup";
import { Order, stableSort, getComparator } from "../../../common/tableUtils";
import { StyledTableCell } from "../../../common/TableCell";
import { MemoryTableRow } from "./MemoryTableRow";

const styles = (theme: Theme) =>
  createStyles({
    table: {
      marginTop: theme.spacing(1),
    },
    cell: {
      padding: theme.spacing(1),
      textAlign: "center",
    },
  });

const mapStateToProps = (state: StoreState) => ({
  tab: state.dashboard.tab,
  memoryTable: state.dashboard.memoryTable,
  shouldObtainMemoryTable: state.dashboard.shouldObtainMemoryTable,
});

const mapDispatchToProps = dashboardActions;

type Props = ReturnType<typeof mapStateToProps> &
  typeof mapDispatchToProps &
  WithStyles<typeof styles>;


const MemoryInfo = (props: Props) => {
  const handlePauseMemoryTable = async () => {
    props.setShouldObtainMemoryTable(!props.shouldObtainMemoryTable);
    if (props.shouldObtainMemoryTable) {
      await stopMemoryTableCollection();
    }
  };

  const pauseButtonIcon = props.shouldObtainMemoryTable ? (
    <PauseIcon />
  ) : (
      <PlayArrowIcon />
    );
  const [isGrouped, setIsGrouped] = useState(true);
  const [order, setOrder] = React.useState<Order>('asc');
  const toggleOrder = () => setOrder(order === 'asc' ? 'desc' : 'asc')
  const [orderBy, setOrderBy] = React.useState<keyof MemoryTableEntry | null>(null);
  const { classes, memoryTable } = props;
  return (
    <React.Fragment>
      {memoryTable !== null ? (
        <React.Fragment>
          <Button color="primary" onClick={handlePauseMemoryTable}>
            {pauseButtonIcon}
            {props.shouldObtainMemoryTable
              ? "Pause Collection"
              : "Resume Collection"}
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
              orderBy={orderBy || ""}
              order={order}
              onRequestSort={(event, property) => {
                if (property === orderBy) {
                  toggleOrder();
                } else {
                  setOrderBy(property);
                  setOrder('asc');
                }
              }}
              headerInfo={memoryHeaderInfo}
            />
            <TableBody>
              {isGrouped
                ? makeGroupedEntries(memoryTable.group)                 
                : makeUngroupedEntries(memoryTable.group, order, orderBy) 
              }
            </TableBody>
          </Table>
        </React.Fragment>
      ) : (
          <div>No Memory Table Information Provided</div>
        )}
    </React.Fragment>
  );
};

function makeGroupedEntries(memoryTableGroups: MemoryTableGroups) {
  return Object.keys(memoryTableGroups).map((group_key, index) => (
    <MemoryRowGroup
      key={index}
      groupKey={group_key}
      memoryTableGroups={memoryTableGroups}
      initialExpanded={true}
    />
  ));
}

function makeUngroupedEntries(memoryTableGroups: MemoryTableGroups, order: Order, orderBy: keyof MemoryTableEntry | null) {
  const allEntries = Object.values(memoryTableGroups).reduce(
                  (allEntries: Array<MemoryTableEntry>, memoryTableGroup) => {
                    const groupEntries = memoryTableGroup.entries;
                    return allEntries.concat(groupEntries)
                  }, []);
  const sortedEntries = orderBy === null
    ? allEntries
    : stableSort(allEntries, getComparator(order, orderBy));
  return sortedEntries.map((memoryTableEntry, index) => (
                        <MemoryTableRow
                          memoryTableEntry={memoryTableEntry}
                          key={`mem-row-${index}`}
                        />
                      ));
}

interface HeaderInfo {
  id: keyof MemoryTableEntry;
  label: string;
  numeric: boolean;
}

const memoryHeaderInfo: HeaderInfo[] =
  [{ id: 'node_ip_address', label: "IP Address", numeric: true },
  { id: 'pid', label: "pid", numeric: true },
  { id: "type", label: "Type", numeric: false },
  { id: "object_id", label: "Object ID", numeric: false },
  { id: "object_size", label: "Object Size (B)", numeric: true },
  { id: "reference_type", label: "Reference Type", numeric: false },
  { id: "call_site", label: "Call Site", numeric: false }]

const useSortableTableHeadStyles = makeStyles((theme: Theme) =>
  createStyles({
    visuallyHidden: {
      border: 0,
      clip: 'rect(0 0 0 0)',
      height: 1,
      margin: -1,
      overflow: 'hidden',
      padding: 0,
      position: 'absolute',
      top: 20,
      width: 1,
    },
  }),
);
type SortableTableHeadProps<T extends HeaderInfo> = {
  // TODO (mfitton) parameterize this type to work with other types besides MemoryTableEntry
  onRequestSort: (event: React.MouseEvent<unknown>, property: keyof MemoryTableEntry) => void;
  order: Order;
  orderBy: string | null;
  headerInfo: T[];
}
function SortableTableHead<T extends HeaderInfo>(props: SortableTableHeadProps<T>) {
  const { order, orderBy, onRequestSort, headerInfo } = props;
  const classes = useSortableTableHeadStyles();
  const createSortHandler = (property: keyof MemoryTableEntry) => (event: React.MouseEvent<unknown>) => {
    onRequestSort(event, property);
  }
  return (
  <TableHead>
    <TableRow>
      {
        headerInfo.map(headerInfo => (
          <StyledTableCell
            key={headerInfo.id}
            align={headerInfo.numeric ? 'right' : 'left'}
            sortDirection={orderBy === headerInfo.id ? order : false}
          >
            <TableSortLabel
              active={orderBy === headerInfo.id}
              direction={orderBy === headerInfo.id ? order : 'asc'}
              onClick={createSortHandler(headerInfo.id)}
            >
              {headerInfo.label}
              {orderBy === headerInfo.id ? (
                <span className={classes.visuallyHidden}>
                  {order === 'desc' ? 'sorted descending' : 'sorted ascending'}
                </span>
              ) : null}
            </TableSortLabel>
          </StyledTableCell>))
      }
    </TableRow>
  </TableHead>
  )
};

export default connect(
  mapStateToProps,
  mapDispatchToProps,
)(withStyles(styles)(MemoryInfo));
