import {
  Box,
  Button,
  ButtonGroup,
  Grid,
  makeStyles,
  Paper,
  Switch,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from "@material-ui/core";
import { SearchOutlined } from "@material-ui/icons";
import Autocomplete from "@material-ui/lab/Autocomplete";
import Pagination from "@material-ui/lab/Pagination";
import dayjs from "dayjs";
import React, { useContext, useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { GlobalContext } from "../App";
import { getEvents, getGlobalEvents } from "../service/event";
import { Event } from "../type/event";
import { useFilter } from "../util/hook";
import LogVirtualView from "./LogView/LogVirtualView";
import { StatusChip } from "./StatusChip";

type EventTableProps = {
  job_id?: string;
};

const useStyles = makeStyles((theme) => ({
  table: {
    marginTop: theme.spacing(4),
    padding: theme.spacing(2),
  },
  pageMeta: {
    padding: theme.spacing(2),
    marginTop: theme.spacing(2),
  },
  filterContainer: {
    display: "flex",
    alignItems: "center",
  },
  search: {
    margin: theme.spacing(1),
    display: "inline-block",
    fontSize: 12,
    lineHeight: "46px",
    height: 56,
  },
  infokv: {
    margin: theme.spacing(1),
  },
  li: {
    color: theme.palette.text.secondary,
    fontSize: 12,
  },
  code: {
    wordBreak: "break-all",
    whiteSpace: "pre-line",
    margin: 12,
    fontSize: 14,
    color: theme.palette.text.primary,
  },
}));

const columns = [
  { label: "Level" },
  { label: "Event type" },
  { label: "Timestamp" },
  {
    label: "Description",
  },
];
const NewEventTable = (props: EventTableProps) => {
  const classes = useStyles();

  return (
    <div style={{ position: "relative" }}>
      <header>
        <TableToolbarContainer>
          <Searchbar
            onFilterChange={setNameFilter}
            placeholder="Search names"
            filter={nameFilter}
          />
          <FiltersToolbarContainer flexGrow={4}>
            <CreatedByFilterPillV2
              defaultFilteredByMe
              context="WorkspaceTable"
            />
          </FiltersToolbarContainer>
        </TableToolbarContainer>
      </header>
      <body>
        <TableContainer>
          <Table>
            <TableHead>
              <TableRow>
                {columns.map(({ label }) => (
                  <TableCell align="center" key={label}>
                    <Box
                      display="flex"
                      justifyContent="center"
                      alignItems="center"
                    >
                      {label}
                    </Box>
                  </TableCell>
                ))}
              </TableRow>
            </TableHead>
            <TableBody></TableBody>
          </Table>
        </TableContainer>
      </body>
      <footer>
        <TablePagination setPage={setPage} />
      </footer>
    </div>
  );
};

export default NewEventTable;
