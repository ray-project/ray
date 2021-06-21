import {
  createStyles,
  makeStyles,
  Paper,
  Table,
  TableBody,
  Theme,
} from "@material-ui/core";
import React from "react";
import { MemoryTableEntry } from "../../../api";
import SortableTableHead, {
  HeaderInfo,
} from "../../../common/SortableTableHead";
import { getComparator, Order, stableSort } from "../../../common/tableUtils";

import { MemoryTableRow } from "./MemoryTableRow";

const useMemoryTableStyles = makeStyles((theme: Theme) =>
  createStyles({
    container: {
      margin: theme.spacing(1),
      padding: theme.spacing(1),
    },
    cell: {
      padding: theme.spacing(1),
      textAlign: "center",
    },
  }),
);

type memoryColumnId =
  | "nodeIpAddress"
  | "pid"
  | "type"
  | "objectRef"
  | "objectSize"
  | "referenceType"
  | "callSite";

const memoryHeaderInfo: HeaderInfo<memoryColumnId>[] = [
  {
    id: "nodeIpAddress",
    label: "IP Address",
    numeric: false,
    sortable: true,
  },
  { id: "pid", label: "PID", numeric: false, sortable: true },
  { id: "type", label: "Type", numeric: false, sortable: true },
  { id: "objectRef", label: "Object Ref", numeric: false, sortable: true },
  {
    id: "objectSize",
    label: "Object Size",
    numeric: false,
    sortable: true,
  },
  {
    id: "referenceType",
    label: "Reference Type",
    numeric: false,
    sortable: true,
  },
  { id: "callSite", label: "Call Site", numeric: false, sortable: true },
];

type MemoryTableProps = {
  tableEntries: MemoryTableEntry[];
};

const MemoryTable: React.FC<MemoryTableProps> = ({ tableEntries }) => {
  const toggleOrder = () => setOrder(order === "asc" ? "desc" : "asc");
  const classes = useMemoryTableStyles();
  const [order, setOrder] = React.useState<Order>("asc");
  const [orderBy, setOrderBy] = React.useState<memoryColumnId | null>(null);
  const comparator = orderBy && getComparator(order, orderBy);
  const sortedTableEntries = comparator
    ? stableSort(tableEntries, comparator)
    : tableEntries;
  const tableRows = sortedTableEntries.map((tableEntry) => (
    <MemoryTableRow memoryTableEntry={tableEntry} key={tableEntry.objectRef} />
  ));
  // Todo(max) add in sorting code
  return (
    <Paper className={classes.container} elevation={2}>
      <Table>
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
        <TableBody>{tableRows}</TableBody>
      </Table>
    </Paper>
  );
};

export default MemoryTable;
