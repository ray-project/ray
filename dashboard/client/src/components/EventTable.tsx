import {
  Button,
  Chip,
  Grid,
  InputAdornment,
  LinearProgress,
  makeStyles,
  TextField,
  TextFieldProps,
  Tooltip,
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
  search: {
    margin: theme.spacing(1),
    display: "inline-block",
    fontSize: 12,
    lineHeight: "46px",
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

const useEventTable = (props: EventTableProps) => {
  const { job_id } = props;
  const { nodeMap } = useContext(GlobalContext);
  const [loading, setLoading] = useState(true);
  const { changeFilter: _changeFilter, filterFunc } = useFilter();
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
        if (job_id) {
          const rsp = await getEvents(job_id);
          if (rsp?.data?.data?.events) {
            setEvents(
              rsp.data.data.events.sort(
                (a, b) => Number(b.timestamp) - Number(a.timestamp),
              ),
            );
          }
        } else {
          const rsp = await getGlobalEvents();
          if (rsp?.data?.data?.events) {
            setEvents(
              Object.values(rsp.data.data.events)
                .reduce((a, b) => a.concat(b))
                .sort((a, b) => Number(b.timestamp) - Number(a.timestamp)),
            );
          }
        }
      } catch (e) {
      } finally {
        setLoading(false);
      }
    };

    getEvent();
  }, [job_id]);

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
    changeFilter,
    pagination,
    changePage,
    labelOptions: Array.from(new Set(events.map((e) => e.label))),
    hostOptions: Array.from(
      new Set(events.map((e) => e.sourceHostname || e.hostName)),
    ),
    sourceOptions: Array.from(new Set(events.map((e) => e.sourceType))),
    severityOptions: Array.from(new Set(events.map((e) => e.severity))),
    loading,
    reverseEvents: () => {
      setEvents([...events.reverse()]);
    },
    nodeMap,
  };
};

const EventTable = (props: EventTableProps) => {
  const classes = useStyles();
  const {
    events,
    changeFilter,
    pagination,
    changePage,
    labelOptions,
    hostOptions,
    sourceOptions,
    severityOptions,
    loading,
    reverseEvents,
    nodeMap,
  } = useEventTable(props);

  if (loading) {
    return <LinearProgress />;
  }

  return (
    <div style={{ position: "relative" }}>
      <div>
        <Autocomplete
          className={classes.search}
          style={{ width: 200 }}
          options={labelOptions}
          onInputChange={(_: any, value: string) => {
            changeFilter("label", value.trim());
          }}
          renderInput={(params: TextFieldProps) => (
            <TextField {...params} label="Label" />
          )}
        />
        <Autocomplete
          className={classes.search}
          style={{ width: 200 }}
          options={hostOptions}
          onInputChange={(_: any, value: string) => {
            changeFilter("sourceHostname", value.trim());
          }}
          renderInput={(params: TextFieldProps) => (
            <TextField {...params} label="Host" />
          )}
        />
        <Autocomplete
          className={classes.search}
          style={{ width: 100 }}
          options={sourceOptions}
          onInputChange={(_: any, value: string) => {
            changeFilter("sourceType", value.trim());
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
            changeFilter("severity", value.trim());
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
        <TextField
          className={classes.search}
          label="Page Size"
          InputProps={{
            onChange: ({ target: { value } }) => {
              changePage("pageSize", Math.min(Number(value), 500) || 10);
            },
            value: pagination.pageSize,
          }}
        />
        <Button className={classes.search} onClick={() => reverseEvents()}>
          Reverse
        </Button>
      </div>
      <div>
        <Pagination
          count={pagination.total}
          page={pagination.pageNo}
          onChange={(event: React.ChangeEvent<unknown>, value: number) => {
            changePage("pageNo", value);
          }}
        />
      </div>
      <div className={classes.pageMeta}>
        {!events.length
          ? "No Events Yet."
          : events.map(
              ({
                label,
                message,
                timestamp,
                timeStamp,
                sourceType,
                sourceHostname,
                hostName,
                sourcePid,
                pid,
                eventId,
                jobId,
                jobName,
                nodeId,
                severity,
                customFields,
              }) => {
                const realTimestamp =
                  timeStamp ||
                  dayjs(Math.floor(timestamp * 1000)).format(
                    "YYYY-MM-DD HH:mm:ss",
                  );
                const hostname = sourceHostname || hostName;
                const realPid = pid || sourcePid;
                return (
                  <article className={classes.li} key={eventId}>
                    <Grid container spacing={4}>
                      <Grid item>
                        <StatusChip status={severity} type={severity} />
                      </Grid>
                      <Grid item>{realTimestamp}</Grid>
                      {customFields && (
                        <Grid item>
                          <Tooltip
                            interactive
                            title={
                              <pre style={{ whiteSpace: "pre-wrap" }}>
                                {JSON.stringify(customFields, null, 2)}
                              </pre>
                            }
                          >
                            <Chip size="small" label="CustomFields" />
                          </Tooltip>
                        </Grid>
                      )}
                    </Grid>
                    <Grid container>
                      <Grid item className={classes.infokv}>
                        severity: {severity}
                      </Grid>
                      <Grid item className={classes.infokv}>
                        source: {sourceType}
                      </Grid>
                      <Grid item className={classes.infokv}>
                        hostname:{" "}
                        {nodeMap[hostname] ? (
                          <Link to={`/node/${nodeMap[hostname]}`}>
                            {hostname}
                          </Link>
                        ) : (
                          hostname
                        )}
                      </Grid>
                      <Grid item className={classes.infokv}>
                        pid: {realPid}
                      </Grid>
                      {jobId && (
                        <Grid item className={classes.infokv}>
                          jobId: <Link to={`/job/${jobId}`}>{jobId}</Link>
                        </Grid>
                      )}
                      {jobName && (
                        <Grid item className={classes.infokv}>
                          jobId: {jobName}
                        </Grid>
                      )}
                      {eventId && (
                        <Grid item className={classes.infokv}>
                          eventId: {eventId}
                        </Grid>
                      )}
                      {nodeId && (
                        <Grid item className={classes.infokv}>
                          nodeId: {nodeId}
                        </Grid>
                      )}
                    </Grid>
                    <LogVirtualView content={message} language="prolog" />
                  </article>
                );
              },
            )}
      </div>
      <div>
        <Pagination
          count={pagination.total}
          page={pagination.pageNo}
          onChange={(event: React.ChangeEvent<unknown>, value: number) => {
            changePage("pageNo", value);
          }}
        />
      </div>
    </div>
  );
};

export default EventTable;
