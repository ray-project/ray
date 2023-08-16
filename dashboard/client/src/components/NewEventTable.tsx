import {
  Box,
  Button,
  ButtonGroup,
  Grid,
  InputAdornment,
  LinearProgress,
  makeStyles,
  Paper,
  Switch,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  TextFieldProps,
  Typography,
} from "@material-ui/core";
import { SearchOutlined } from "@material-ui/icons";
import Autocomplete from "@material-ui/lab/Autocomplete";
import Pagination from "@material-ui/lab/Pagination";
import dayjs from "dayjs";
import React, { useContext, useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { GlobalContext } from "../App";
import { CodeDialogButtonWithPreview } from "../common/CodeDialogButton";
import { StyledTableCell } from "../common/TableCell";
import { getEvents, getGlobalEvents, getNewEvents } from "../service/event";
import { Event } from "../type/event";
import { useFilter } from "../util/hook";
import { MOCK_DATA } from "./EventTableMockData";
import LogVirtualView from "./LogView/LogVirtualView";
import { StatusChip } from "./StatusChip";

type EventTableProps = {
  defaultSeverityLevels?: string[];
};

const transformFiltersToParams = (filters: Filters) => {
  const params = new URLSearchParams();
  if (!filters) {
    return;
  }

  const isString = (value: any): value is string => typeof value === "string";

  for (const key in filters) {
    const t = typeof filters.entityName;
    console.info("t : ", t);

    if (!key) {
      continue;
    }

    if (
      isString(key) &&
      key === "entityId" &&
      filters.entityName !== undefined &&
      filters.entityId !== undefined
    ) {
      params.append(
        `${encodeURIComponent(filters.entityName)}_id`,
        encodeURIComponent(filters.entityId),
      );
    } else if (Array.isArray(filters[key as keyof Filters])) {
      // Process sourceType and severityLevel
      const filterArray = filters[key as keyof Filters] as string[];
      if (filterArray !== undefined) {
        filterArray.forEach((value) => {
          params.append(encodeURIComponent(key), encodeURIComponent(value));
        });
      }
    }
  }

  return params.toString();
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

  tableContainer: {
    overflowX: "scroll",
  },
  expandCollapseIcon: {
    color: theme.palette.text.secondary,
    fontSize: "1.5em",
    verticalAlign: "middle",
  },
  idCol: {
    display: "block",
    width: "50px",
    overflow: "hidden",
    textOverflow: "ellipsis",
    whiteSpace: "nowrap",
  },
  OverflowCol: {
    display: "block",
    width: "100px",
    overflow: "hidden",
    textOverflow: "ellipsis",
    whiteSpace: "nowrap",
  },
  helpInfo: {
    marginLeft: theme.spacing(1),
  },
  message: {
    maxWidth: "200",
  },
}));

const columns = [
  { label: "Severity" },
  { label: "Timestamp" },
  { label: "Source" },
  { label: "Hostname" },
  {
    label: "PID",
  },
  { label: "Message" },
];

type Filters = {
  sourceType: string[]; // TODO: Chao, multi-select severity level in filters button is a P1
  severityLevel: string[]; // TODO: Chao, multi-select severity level in filters button is a P1
  entityName: string | undefined;
  entityId: string | undefined;
};
const useEventTable = (props: EventTableProps) => {
  const { defaultSeverityLevels } = props;
  const { nodeMap } = useContext(GlobalContext);
  const [loading, setLoading] = useState(true);
  const { changeFilter: _changeFilter, filterFunc } = useFilter();
  const [filters, setFilters] = useState<Filters>({
    sourceType: [],
    severityLevel: defaultSeverityLevels || [],
    entityName: undefined, // We used two fields because we will support select entityName by dropdown and input entityId by TextField in the future.
    entityId: undefined, // id or *
  });

  const [events, setEvents] = useState<Event[]>([]);
  const [pagination, setPagination] = useState({
    pageNo: 1,
    pageSize: 10,
    total: 0,
  });

  const changePage = (key: string, value: number) => {
    setPagination({ ...pagination, [key]: value });
  };
  const realLen = events.filter(filterFunc).length;
  const { pageSize } = pagination;
  const changeFilter: typeof _changeFilter = (...params) => {
    _changeFilter(...params);
    setPagination({
      ...pagination,
      pageNo: 1,
    });
  };

  useEffect(() => {
    const getEvent = async () => {
      try {
        const params = transformFiltersToParams(filters);
        const rsp = await getNewEvents(params);
        console.info("rsp: ", rsp);
        const events = rsp?.data?.data?.result?.result;
        if (events) {
          setEvents(
            events.sort((a, b) => Number(b.timestamp) - Number(a.timestamp)),
          );
        }
      } catch (e) {
        console.error("getEvent error: ", e);
      } finally {
        setLoading(false);
      }
    };
    getEvent();
  }, [filters]);

  // useEffect(() => {
  //   const getEvent = async () => {
  //     try {
  //       if (job_id) {
  //         const rsp = await getEvents(job_id);
  //         if (rsp?.data?.data?.events) {
  //           setEvents(
  //             rsp.data.data.events.sort(
  //               (a, b) => Number(b.timestamp) - Number(a.timestamp),
  //             ),
  //           );
  //         }
  //       } else {
  //         const rsp = await getGlobalEvents();
  //         if (rsp?.data?.data?.events) {
  //           setEvents(
  //             Object.values(rsp.data.data.events)
  //               .reduce((a, b) => a.concat(b))
  //               .sort((a, b) => Number(b.timestamp) - Number(a.timestamp)),
  //           );
  //         }
  //       }
  //     } catch (e) {
  //     } finally {
  //       setLoading(false);
  //     }
  //   setEvents(MOCK_DATA.data.events["64000000"] as any);
  //   setLoading(false);

  //   // getEvent();
  // }, []);

  useEffect(() => {
    setPagination((p) => ({
      ...p,
      total: Math.ceil(realLen / p.pageSize),
      pageNo: 1,
    }));
  }, [realLen, pageSize]);

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
    sourceOptions: Array.from(new Set(events.map((e) => e.sourceType))),
    severityOptions: Array.from(new Set(events.map((e) => e.severity))),
    loading,
    nodeMap,
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
    sourceOptions,
    severityOptions,
    loading,
    nodeMap,
  } = useEventTable(props);

  if (loading) {
    return <LinearProgress />;
  }
  return (
    <div>
      <header className={classes.filterContainer}>
        <button onClick={() => setFilters({ ...filters, sourceType: ["GCS"] })}>
          test
        </button>
        <Autocomplete
          className={classes.search}
          style={{ width: 100 }}
          options={sourceOptions}
          onInputChange={(_: any, value: string) => {
            setFilters({ ...filters, sourceType: [value.trim()] });
          }}
          renderInput={(params: TextFieldProps) => (
            <TextField {...params} label="Source" />
          )}
        />
        <Autocomplete
          className={classes.search}
          style={{ width: 140 }}
          options={severityOptions}
          onInputChange={(_: any, value: string) => {
            setFilters({ ...filters, severityLevel: [value.trim()] });
          }}
          renderInput={(params: TextFieldProps) => (
            <TextField {...params} label="Severity" />
          )}
        />
        <TextField
          className={classes.search}
          label="Msg"
          InputProps={{
            onChange: ({ target: { value } }) => {
              changeFilter("message", value.trim());
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
            <TableBody>
              {events.map(
                ({
                  severity,
                  // time,
                  sourceType,
                  hostName,
                  message,
                  sourceHostname,
                  pid,
                  sourcePid,
                  // custom_fields,
                }) => {
                  // const realTimestamp =
                  //   time ||
                  //   dayjs(Math.floor(timestamp * 1000)).format(
                  //     "YYYY-MM-DD HH:mm:ss",
                  //   );
                  return (
                    <React.Fragment>
                      <TableRow>
                        <StyledTableCell>
                          <StatusChip status={severity} type={severity} />
                        </StyledTableCell>
                        {/* <StyledTableCell>{time}</StyledTableCell> */}
                        <StyledTableCell>{sourceType}</StyledTableCell>
                        {/* <StyledTableCell>{custom_fields}</StyledTableCell> */}
                        <StyledTableCell>
                          {message ? (
                            <CodeDialogButtonWithPreview
                              className={classes.message}
                              buttonText={"Expand"}
                              title="Event Message Detail"
                              code={message}
                            />
                          ) : (
                            "-"
                          )}
                        </StyledTableCell>
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
