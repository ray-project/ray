import {
  createStyles,
  makeStyles,
  TableHead,
  TableRow,
  TableSortLabel,
} from "@material-ui/core";
import React from "react";
import { StyledTableCell } from "./TableCell";
import { Order } from "./tableUtils";

const useSortableTableHeadStyles = makeStyles(() =>
  createStyles({
    visuallyHidden: {
      border: 0,
      clip: "rect(0 0 0 0)",
      height: 1,
      margin: -1,
      overflow: "hidden",
      padding: 0,
      position: "absolute",
      top: 20,
      width: 1,
    },
  }),
);

export type HeaderInfo<T> = {
  sortable: boolean;
  id: T;
  label: string;
  numeric: boolean;
};

type SortableTableHeadProps<T> = {
  onRequestSort: (event: React.MouseEvent<unknown>, id: T) => void;
  order: Order;
  orderBy: T | null;
  headerInfo: HeaderInfo<T>[];
  firstColumnEmpty: boolean;
};

const SortableTableHead = <T,>(props: SortableTableHeadProps<T>) => {
  const { order, orderBy, onRequestSort, headerInfo, firstColumnEmpty } = props;
  const classes = useSortableTableHeadStyles();
  const createSortHandler = (id: T) => (event: React.MouseEvent<unknown>) => {
    onRequestSort(event, id);
  };
  return (
    <TableHead>
      <TableRow>
        {firstColumnEmpty && <StyledTableCell />}
        {headerInfo.map((headerInfo) => {
          if (headerInfo.sortable) {
            return (
              <StyledTableCell
                key={headerInfo.label}
                align={headerInfo.numeric ? "right" : "left"}
                sortDirection={orderBy === headerInfo.id ? order : false}
              >
                <TableSortLabel
                  active={orderBy === headerInfo.id}
                  direction={orderBy === headerInfo.id ? order : "asc"}
                  onClick={createSortHandler(headerInfo.id)}
                >
                  {headerInfo.label}
                  {orderBy === headerInfo.id ? (
                    <span className={classes.visuallyHidden}>
                      {order === "desc"
                        ? "sorted descending"
                        : "sorted ascending"}
                    </span>
                  ) : null}
                </TableSortLabel>
              </StyledTableCell>
            );
          } else {
            return (
              <StyledTableCell
                key={headerInfo.label}
                align={headerInfo.numeric ? "right" : "left"}
              >
                {headerInfo.label}
              </StyledTableCell>
            );
          }
        })}
      </TableRow>
    </TableHead>
  );
};

export default SortableTableHead;
