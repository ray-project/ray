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
import React, { useEffect, useState } from "react";
import { getEvents } from "../service/event";
import { Align, Event, Filters, SeverityLevel } from "../type/event";
import { useFilter } from "../util/hook";
import { StatusChip } from "./StatusChip";

type EventTableProps = {
  defaultSeverityLevels?: SeverityLevel[];
  entityName?: string;
  entityId?: string; // It could be a specific or "*" to represent all entities
};

const transformFiltersToParams = (filters: Filters) => {
  const params = new URLSearchParams();

  if (filters.entityName && filters.entityId) {
    params.append(
      encodeURIComponent(filters.entityName),
      encodeURIComponent(filters.entityId),
    );
  }

  for (const key in filters) {
    // Skip entityName and entityId
    if (key === "entityName" || key === "entityId") {
      continue;
    }
    if (key === "sourceType" || key === "severityLevel") {
      const filterArray = filters[key as keyof Filters] as string[];
      filterArray.forEach((value) => {
        params.append(encodeURIComponent(key), encodeURIComponent(value));
      });
    } else {
      // key === 'count' or other key to add in the future
      params.append(
        encodeURIComponent(key),
        encodeURIComponent(filters[key as keyof Filters] as string),
      );
    }
  }

  return params.toString();
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
}));

const SOURCE_TYPE_OPTIONS = [
  "common",
  "core_worker",
  "gcs",
  "raylet",
  "jobs",
  "serve",
  "cluster_lifecycle",
  "autoscaler",
];

const SEVERITY_LEVEL_OPTIONS = ["info", "debug", "warning", "error", "tracing"];

const COLUMNS = [
  { label: "Severity", align: "center" },
  { label: "Timestamp", align: "center" },
  { label: "Source", align: "center" },
  { label: "Custom Fields", align: "left" },
  { label: "Message", align: "left" },
];

const useEventTable = (props: EventTableProps) => {
  const { defaultSeverityLevels, entityName, entityId } = props;
  const [loading, setLoading] = useState(true);
  const { changeFilter: _changeFilter, filterFunc } = useFilter();
  const [filters, _setFilters] = useState<Filters>({
    sourceType: [],
    severityLevel: defaultSeverityLevels || [],
    entityName, // We used two fields(entityName, entityId) because we will support select entityName by dropdown and input entityId by TextField in the future.
    entityId, // id or *
  });

  const [events, setEvents] = useState<Event[]>([]);

  const [pagination, setPagination] = useState({
    pageNo: 1,
    pageSize: 10,
    total: 0,
  });
  const { pageSize } = pagination;

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

  useEffect(() => {
    const getEvent = async () => {
      try {
        const params = transformFiltersToParams(filters);
        const rsp = await getEvents(params); // We don't useSWR since we need to get real time events data once filters changed
        const events = rsp?.data?.data?.result;
        if (events) {
          setEvents(events); // We sort the event by timestamp in the backend
        }
      } catch (e) {
        console.error("getEvent error: ", e);
      } finally {
        setLoading(false);
      }
    };
    getEvent();
  }, [filters]);

  const range = [
    (pagination.pageNo - 1) * pagination.pageSize,
    pagination.pageNo * pagination.pageSize,
  ];

  return {
    events: events.filter(filterFunc).slice(range[0], range[1]),
    filters,
    setFilters,
    changeFilter,
    pagination,
    changePage,
    loading,
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
  } = useEventTable(props);

  if (loading) {
    return <LinearProgress />;
  }
  return (
    <div>
      <header className={classes.filterContainer}>
        <Autocomplete
          className={classes.search}
          style={{ width: 150 }}
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
      <body>
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
                  severity,
                  sourceType,
                  timestamp,
                  message,
                  customFields,
                }) => {
                  const realTimestamp = dayjs(
                    Math.floor(timestamp * 1000),
                  ).format("YYYY-MM-DD HH:mm:ss");
                  const customFieldsDisplay =
                    customFields && Object.keys(customFields).length > 0
                      ? JSON.stringify(customFields)
                      : "-";
                  return (
                    <React.Fragment>
                      <TableRow>
                        <TableCell align="center">
                          <StatusChip status={severity} type={severity} />
                        </TableCell>
                        <TableCell align="center">{realTimestamp}</TableCell>
                        <TableCell align="center">{sourceType}</TableCell>
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
      </body>
      <footer>
        <Pagination
          className={classes.pagination}
          count={pagination.total}
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
