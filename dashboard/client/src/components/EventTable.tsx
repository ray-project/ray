import { SearchOutlined } from "@mui/icons-material";
import {
  Button,
  Chip,
  Grid,
  InputAdornment,
  LinearProgress,
  TextField,
  TextFieldProps,
  Tooltip,
} from "@mui/material";
import Autocomplete from "@mui/material/Autocomplete";
import Pagination from "@mui/material/Pagination";
import { styled } from "@mui/material/styles";
import dayjs from "dayjs";
import React, { useContext, useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { GlobalContext } from "../App";
import { sliceToPage } from "../common/util";
import { getEvents, getGlobalEvents } from "../service/event";
import { Event } from "../type/event";
import { useFilter } from "../util/hook";
import LogVirtualView from "./LogView/LogVirtualView";
import { StatusChip } from "./StatusChip";

type EventTableProps = {
  job_id?: string;
};

const PageMetaDiv = styled("div")(({ theme }) => ({
  padding: theme.spacing(2),
  marginTop: theme.spacing(2),
}));

const FilterContainerDiv = styled("div")(({ theme }) => ({
  display: "flex",
  alignItems: "center",
}));

const SearchAutocomplete = styled(Autocomplete)(({ theme }) => ({
  margin: theme.spacing(1),
  display: "inline-block",
  fontSize: 12,
}));

const SearchTextField = styled(TextField)(({ theme }) => ({
  margin: theme.spacing(1),
  display: "inline-block",
  fontSize: 12,
}));

const SearchButton = styled(Button)(({ theme }) => ({
  margin: theme.spacing(1),
  display: "inline-block",
  fontSize: 12,
}));

const InfoKVGrid = styled(Grid)(({ theme }) => ({
  margin: theme.spacing(1),
}));

const LiArticle = styled("article")(({ theme }) => ({
  color: theme.palette.text.secondary,
  fontSize: 12,
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

  return {
    events: sliceToPage(
      events.filter(filterFunc),
      pagination.pageNo,
      pagination.pageSize,
    ).items,
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
      <FilterContainerDiv>
        <SearchAutocomplete
          style={{ width: 200 }}
          options={labelOptions}
          onInputChange={(_: any, value: string) => {
            changeFilter("label", value.trim());
          }}
          renderInput={(params: TextFieldProps) => (
            <TextField {...params} label="Label" />
          )}
        />
        <SearchAutocomplete
          style={{ width: 200 }}
          options={hostOptions}
          onInputChange={(_: any, value: string) => {
            changeFilter("sourceHostname", value.trim());
          }}
          renderInput={(params: TextFieldProps) => (
            <TextField {...params} label="Host" />
          )}
        />
        <SearchAutocomplete
          style={{ width: 100 }}
          options={sourceOptions}
          onInputChange={(_: any, value: string) => {
            changeFilter("sourceType", value.trim());
          }}
          renderInput={(params: TextFieldProps) => (
            <TextField {...params} label="Source" />
          )}
        />
        <SearchAutocomplete
          style={{ width: 140 }}
          options={severityOptions}
          onInputChange={(_: any, value: string) => {
            changeFilter("severity", value.trim());
          }}
          renderInput={(params: TextFieldProps) => (
            <TextField {...params} label="Severity" />
          )}
        />
        <SearchTextField
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
          label="Page Size"
          sx={{ margin: 1, width: 120 }}
          size="small"
          defaultValue={10}
          InputProps={{
            onChange: ({ target: { value } }) => {
              changePage("pageSize", Math.min(Number(value), 500) || 10);
            },
            endAdornment: (
              <InputAdornment position="end">Per Page</InputAdornment>
            ),
          }}
        />
        <SearchButton
          size="small"
          variant="contained"
          onClick={() => reverseEvents()}
        >
          Reverse
        </SearchButton>
      </FilterContainerDiv>
      <div>
        <Pagination
          count={pagination.total}
          page={pagination.pageNo}
          onChange={(event: React.ChangeEvent<unknown>, value: number) => {
            changePage("pageNo", value);
          }}
        />
      </div>
      <PageMetaDiv>
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
                  <LiArticle key={eventId}>
                    <Grid container spacing={4}>
                      <Grid item>
                        <StatusChip status={severity} type={severity} />
                      </Grid>
                      <Grid item>{realTimestamp}</Grid>
                      {customFields && (
                        <Grid item>
                          <Tooltip
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
                      <InfoKVGrid item>severity: {severity}</InfoKVGrid>
                      <InfoKVGrid item>source: {sourceType}</InfoKVGrid>
                      <InfoKVGrid item>
                        hostname:{" "}
                        {nodeMap[hostname] ? (
                          <Link to={`/node/${nodeMap[hostname]}`}>
                            {hostname}
                          </Link>
                        ) : (
                          hostname
                        )}
                      </InfoKVGrid>
                      <InfoKVGrid item>pid: {realPid}</InfoKVGrid>
                      {jobId && (
                        <InfoKVGrid item>
                          jobId: <Link to={`/job/${jobId}`}>{jobId}</Link>
                        </InfoKVGrid>
                      )}
                      {jobName && (
                        <InfoKVGrid item>jobId: {jobName}</InfoKVGrid>
                      )}
                      {eventId && (
                        <InfoKVGrid item>eventId: {eventId}</InfoKVGrid>
                      )}
                      {nodeId && <InfoKVGrid item>nodeId: {nodeId}</InfoKVGrid>}
                    </Grid>
                    <LogVirtualView content={message} language="prolog" />
                  </LiArticle>
                );
              },
            )}
      </PageMetaDiv>
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
