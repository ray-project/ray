import { Order } from "./tableUtils";
import { StyledTableCell } from "./TableCell";
import { makeStyles, Theme, createStyles, TableHead, TableRow, TableSortLabel } from "@material-ui/core";
import React from "react";

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

export interface HeaderInfo<T> {
  id: keyof T;
  label: string;
  numeric: boolean;
}

type SortableTableHeadProps<T> = {
  // TODO (mfitton) parameterize this type to work with other types besides MemoryTableEntry
  onRequestSort: (event: React.MouseEvent<unknown>, property: keyof T) => void;
  order: Order;
  orderBy: string | null;
  headerInfo: HeaderInfo<T>[];
}

function SortableTableHead<T>(props: SortableTableHeadProps<T>) {
  const { order, orderBy, onRequestSort, headerInfo } = props;
  const classes = useSortableTableHeadStyles();
  const createSortHandler = (property: keyof T) => (event: React.MouseEvent<unknown>) => {
    onRequestSort(event, property);
  }
  return (
  <TableHead>
    <TableRow>
      {
        headerInfo.map(headerInfo => (
          <StyledTableCell
            key={headerInfo.label}
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

export default SortableTableHead