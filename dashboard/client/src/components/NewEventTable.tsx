import {
  Box,
  InputAdornment,
  LinearProgress,
  makeStyles,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  TextFieldProps,
  Tooltip,
} from "@material-ui/core";
import { SearchOutlined } from "@material-ui/icons";
import Autocomplete from "@material-ui/lab/Autocomplete";
import Pagination from "@material-ui/lab/Pagination";
import dayjs from "dayjs";
import React, { useState } from "react";
import { Align, Filters } from "../type/event";

import { useFilter } from "../util/hook";
import { SeverityLevel } from "./event";
import Loading from "./Loading";
import { StatusChip } from "./StatusChip";
import { useEvents } from "./useEvents";

type EventTableProps = {
  defaultSeverityLevels?: SeverityLevel[];
  entityName?: string;
  entityId?: string; // It could be a specific or "*" to represent all entities
};

const useStyles = makeStyles((theme) => ({
  overflowCell: {
    display: "block",
    margin: "auto",
    maxWidth: 360,
    textOverflow: "ellipsis",
    overflow: "hidden",
    whiteSpace: "nowrap",
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

  tableContainer: {
    overflowX: "scroll",
  },

  helpInfo: {
    marginLeft: theme.spacing(1),
  },
  message: {
    maxWidth: 200,
  },
  pagination: {
    marginTop: theme.spacing(3),
  },
  root: {
    height: 800,
    paddingLeft: theme.spacing(1),
  },
}));

const SOURCE_TYPE_OPTIONS = [
  "COMMON",
  "CORE_WORKER",
  "GCS",
  "RAYLET",
  "JOBS",
  "SERVE",
  "CLUSTER_LIFECYCLE",
  "AUTOSCALER",
];

const SEVERITY_LEVEL_OPTIONS = ["INFO", "DEBUG", "WARNING", "ERROR", "TRACING"];

const COLUMNS = [
  { label: "Severity", align: "center" },
  { label: "Timestamp", align: "center" },
  { label: "Source", align: "center" },
  { label: "Custom fields", align: "left" },
  { label: "Message", align: "left" },
];

const useEventTable = (props: EventTableProps) => {
  const { defaultSeverityLevels, entityName, entityId } = props;
  const { changeFilter: _changeFilter, filterFunc } = useFilter();
  const [filters, _setFilters] = useState<Filters>({
    sourceType: [],
    severityLevel: defaultSeverityLevels || [],
    entityName, // We used two fields(entityName, entityId) because we will support select entityName by dropdown and input entityId by TextField in the future.
    entityId, // id or *
  });

  const [pagination, setPagination] = useState({
    pageNo: 1, // first page is PageNo 1
    pageSize: 10,
  });

  const changePage = (key: string, value: number) => {
    setPagination({ ...pagination, [key]: value });
  };

  const changeFilter: typeof _changeFilter = (...params) => {
    _changeFilter(...params);
    setPagination({
      ...pagination,
      pageNo: 1,
    });
  };

  const setFilters: typeof _setFilters = (...params) => {
    _setFilters(...params);
    setPagination({
      ...pagination,
      pageNo: 1,
    });
  };
  const { pageNo } = pagination;

  const {
    data: eventsData = [],
    error,
    isLoading,
  } = useEvents(filters, pageNo);
  console.error(error, "getEvents error");

  const range = [
    (pagination.pageNo - 1) * pagination.pageSize,
    pagination.pageNo * pagination.pageSize,
  ];

  return {
    total: eventsData.filter(filterFunc).length,
    events: eventsData.filter(filterFunc).slice(range[0], range[1]),
    filters,
    setFilters,
    changeFilter,
    pagination,
    changePage,
    loading: isLoading,
  };
};

const NewEventTable = (props: EventTableProps) => {
  const classes = useStyles();
  const {
    events,
    filters,
    setFilters,
    changeFilter,
    pagination,
    changePage,
    loading,
    total,
  } = useEventTable(props);

  if (loading) {
    return <Loading loading={loading} />;
  }

  const controlHeader = (
    <header className={classes.filterContainer}>
      <Autocomplete
        className={classes.search}
        style={{ width: 150 }}
        value={filters.severityLevel?.[0]}
        options={SEVERITY_LEVEL_OPTIONS}
        onInputChange={(_: any, value: string) => {
          setFilters({ ...filters, severityLevel: [value.trim()] });
        }}
        renderInput={(params: TextFieldProps) => (
          <TextField {...params} label="Severity" />
        )}
      />
      <Autocomplete
        className={classes.search}
        style={{ width: 150 }}
        value={filters.sourceType?.[0]}
        options={SOURCE_TYPE_OPTIONS}
        onInputChange={(_: any, value: string) => {
          setFilters({ ...filters, sourceType: [value.trim()] });
        }}
        renderInput={(params: TextFieldProps) => (
          <TextField {...params} label="Source" />
        )}
      />
      <TextField
        className={classes.search}
        label="Message"
        InputProps={{
          onChange: ({ target: { value } }) => {
            changeFilter("message", value.trim()); // TODO: filter the message in the frontend and to filter it in the backend in the future
          },
          endAdornment: (
            <InputAdornment position="end">
              <SearchOutlined />
            </InputAdornment>
          ),
        }}
      />
    </header>
  );
  const eventsLen = events.length;
  if (eventsLen < 1) {
    return (
      <div>
        {controlHeader}
        <p className={classes.root}>No events</p>;
      </div>
    );
  }

  return (
    <div className={classes.root}>
      {controlHeader}
      <div>
        <TableContainer component={Paper}>
          <Table className={classes.tableContainer}>
            <TableHead>
              <TableRow>
                {COLUMNS.map(({ label, align }) => (
                  <TableCell key={label} align={align as Align}>
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
            <TableBody>
              {events.map(
                ({
                  event_id,
                  severity,
                  source_type,
                  timestamp,
                  message,
                  custom_fields,
                }) => {
                  const realTimestamp = dayjs(
                    Math.floor(timestamp * 1000),
                  ).format("YYYY-MM-DD HH:mm:ss");
                  const customFieldsDisplay =
                    custom_fields && Object.keys(custom_fields).length > 0
                      ? JSON.stringify(custom_fields)
                      : "-";
                  return (
                    <React.Fragment key={event_id}>
                      <TableRow>
                        <TableCell align="center">
                          <StatusChip status={severity} type={severity} />
                        </TableCell>
                        <TableCell align="center">{realTimestamp}</TableCell>
                        <TableCell align="center">{source_type}</TableCell>
                        <TableCell align="left">
                          <Tooltip
                            className={classes.overflowCell}
                            title={customFieldsDisplay}
                            arrow
                            interactive
                          >
                            <div>{customFieldsDisplay}</div>
                          </Tooltip>
                        </TableCell>
                        <TableCell align="left">
                          <Tooltip
                            className={classes.overflowCell}
                            title={message}
                            arrow
                            interactive
                          >
                            <div>{message}</div>
                          </Tooltip>
                        </TableCell>
                      </TableRow>
                    </React.Fragment>
                  );
                },
              )}
            </TableBody>
          </Table>
        </TableContainer>
      </div>
      <footer>
        <Pagination
          className={classes.pagination}
          count={total > 0 ? Math.ceil(total / pagination.pageSize) : 0}
          page={pagination.pageNo}
          onChange={(event: React.ChangeEvent<unknown>, value: number) => {
            changePage("pageNo", value);
          }}
        />
      </footer>
    </div>
  );
};

export default NewEventTable;
