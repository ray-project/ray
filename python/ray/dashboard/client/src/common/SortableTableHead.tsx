import {
  createStyles,
  makeStyles,
  TableHead,
  TableRow,
  TableSortLabel,
  Theme,
} from "@material-ui/core";
import React from "react";
import { StyledTableCell } from "./TableCell";
import { Order } from "./tableUtils";

const useSortableTableHeadStyles = makeStyles((theme: Theme) =>
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

export type HeaderInfo = {
  sortable: boolean;
  id: string;
  label: string;
  numeric: boolean;
};

type SortableTableHeadProps = {
  onRequestSort: (event: React.MouseEvent<unknown>, id: string) => void;
  order: Order;
  orderBy: string | null;
  headerInfo: HeaderInfo[];
};

const SortableTableHead = (props: SortableTableHeadProps) => {
  const { order, orderBy, onRequestSort, headerInfo} = props;
  const classes = useSortableTableHeadStyles();
  const createSortHandler = (id: string) => (
    event: React.MouseEvent<unknown>,
  ) => {
    onRequestSort(event, id);
  };
  return (
    <TableHead>
      <TableRow>
        {headerInfo.map(headerInfo => {
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
                      {order === "desc" ? "sorted descending" : "sorted ascending"}
                    </span>
                  ) : null}
                </TableSortLabel>
              </StyledTableCell>
            )
          } else {
              return (
                <StyledTableCell
                  key={headerInfo.label}
                  align={headerInfo.numeric ? "right" : "left"}>
                  {headerInfo.label}
                </StyledTableCell>
              )
          }
        })}
      </TableRow>
    </TableHead>
  );
};

export default SortableTableHead;
