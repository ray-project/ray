import {
  InputAdornment,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TextField,
  TextFieldProps,
  Tooltip,
} from "@material-ui/core";
import { orange } from "@material-ui/core/colors";
import { SearchOutlined } from "@material-ui/icons";
import Autocomplete from "@material-ui/lab/Autocomplete";
import Pagination from "@material-ui/lab/Pagination";
import React, { useContext, useState } from "react";
import { Link } from "react-router-dom";
import { GlobalContext } from "../App";
import { DurationText } from "../common/DurationText";
import rowStyles from "../common/RowStyles";
import { Actor } from "../type/actor";
import { Worker } from "../type/worker";
import { useFilter } from "../util/hook";
import StateCounter from "./StatesCounter";
import { StatusChip } from "./StatusChip";
import RayletWorkerTable, { ExpandableTableRow } from "./WorkerTable";

const ActorTable = ({
  actors = {},
  workers = [],
}: {
  actors: { [actorId: string]: Actor };
  workers?: Worker[];
}) => {
  const [pageNo, setPageNo] = useState(1);
  const { changeFilter, filterFunc } = useFilter();
  const [pageSize, setPageSize] = useState(10);
  const { ipLogMap } = useContext(GlobalContext);
  const actorList = Object.values(actors || {}).filter(filterFunc);
  const list = actorList.slice((pageNo - 1) * pageSize, pageNo * pageSize);
  const classes = rowStyles();

  return (
    <React.Fragment>
      <div style={{ flex: 1, display: "flex", alignItems: "center" }}>
        <Autocomplete
          style={{ margin: 8, width: 120 }}
          options={Array.from(
            new Set(Object.values(actors).map((e) => e.state)),
          )}
          onInputChange={(_: any, value: string) => {
            changeFilter("state", value.trim());
          }}
          renderInput={(params: TextFieldProps) => (
            <TextField {...params} label="State" />
          )}
        />
        <Autocomplete
          style={{ margin: 8, width: 150 }}
          options={Array.from(
            new Set(Object.values(actors).map((e) => e.address?.ipAddress)),
          )}
          onInputChange={(_: any, value: string) => {
            changeFilter("address.ipAddress", value.trim());
          }}
          renderInput={(params: TextFieldProps) => (
            <TextField {...params} label="IP" />
          )}
        />
        <TextField
          style={{ margin: 8, width: 120 }}
          label="PID"
          size="small"
          InputProps={{
            onChange: ({ target: { value } }) => {
              changeFilter("pid", value.trim());
            },
            endAdornment: (
              <InputAdornment position="end">
                <SearchOutlined />
              </InputAdornment>
            ),
          }}
        />
        <TextField
          style={{ margin: 8, width: 120 }}
          label="Name"
          size="small"
          InputProps={{
            onChange: ({ target: { value } }) => {
              changeFilter("name", value.trim());
            },
            endAdornment: (
              <InputAdornment position="end">
                <SearchOutlined />
              </InputAdornment>
            ),
          }}
        />
        <TextField
          style={{ margin: 8, width: 120 }}
          label="Actor ID"
          size="small"
          InputProps={{
            onChange: ({ target: { value } }) => {
              changeFilter("actorId", value.trim());
            },
            endAdornment: (
              <InputAdornment position="end">
                <SearchOutlined />
              </InputAdornment>
            ),
          }}
        />
        <TextField
          style={{ margin: 8, width: 120 }}
          label="Page Size"
          size="small"
          defaultValue={10}
          InputProps={{
            onChange: ({ target: { value } }) => {
              setPageSize(Math.min(Number(value), 500) || 10);
            },
            endAdornment: (
              <InputAdornment position="end">Per Page</InputAdornment>
            ),
          }}
        />
      </div>
      <div style={{ display: "flex", alignItems: "center" }}>
        <div>
          <Pagination
            page={pageNo}
            onChange={(e, num) => setPageNo(num)}
            count={Math.ceil(actorList.length / pageSize)}
          />
        </div>
        <div>
          <StateCounter type="actor" list={actorList} />
        </div>
      </div>
      <Table>
        <TableHead>
          <TableRow>
            {[
              "",
              "ID",
              "Class",
              "Name",
              "State",
              "Uptime",
              "Job Id",
              "Pid",
              "IP",
              "Restarted",
              "Required Resources",
              "Exit Detail",
              "Log",
            ].map((col) => (
              <TableCell align="center" key={col}>
                {col}
              </TableCell>
            ))}
          </TableRow>
        </TableHead>
        <TableBody>
          {list.map(
            ({
              actorId,
              actorClass,
              jobId,
              pid,
              address,
              state,
              name,
              numRestarts,
              startTime,
              endTime,
              exitDetail,
              requiredResources,
            }) => (
              <ExpandableTableRow
                length={
                  workers.filter(
                    (e) =>
                      e.pid === pid &&
                      address.ipAddress === e.coreWorkerStats[0].ipAddress,
                  ).length
                }
                expandComponent={
                  <RayletWorkerTable
                    actorMap={{}}
                    workers={workers.filter(
                      (e) =>
                        e.pid === pid &&
                        address.ipAddress === e.coreWorkerStats[0].ipAddress,
                    )}
                    mini
                  />
                }
                key={actorId}
              >
                <TableCell align="center">
                  <Tooltip
                    className={classes.idCol}
                    title={actorId}
                    arrow
                    interactive
                  >
                    <div>{actorId}</div>
                  </Tooltip>
                </TableCell>
                <TableCell align="center">{actorClass}</TableCell>
                <TableCell align="center">{name ? name : "-"}</TableCell>
                <TableCell align="center">
                  <StatusChip type="actor" status={state} />
                </TableCell>
                <TableCell align="center">
                  {startTime && startTime > 0 ? (
                    <DurationText startTime={startTime} endTime={endTime} />
                  ) : (
                    "-"
                  )}
                </TableCell>
                <TableCell align="center">{jobId}</TableCell>
                <TableCell align="center">{pid ? pid : "-"}</TableCell>
                <TableCell align="center">
                  {address?.ipAddress ? address?.ipAddress : "-"}
                </TableCell>
                <TableCell
                  align="center"
                  style={{
                    color: Number(numRestarts) > 0 ? orange[500] : "inherit",
                  }}
                >
                  {numRestarts}
                </TableCell>
                <TableCell align="center">
                  <Tooltip
                    className={classes.OverflowCol}
                    title={Object.entries(requiredResources || {}).map(
                      ([key, val]) => (
                        <div style={{ margin: 4 }}>
                          {key}: {val}
                        </div>
                      ),
                    )}
                    arrow
                    interactive
                  >
                    <div>
                      {Object.entries(requiredResources || {}).map(
                        ([key, val]) => `${key}: ${val}\n`,
                      )}
                    </div>
                  </Tooltip>
                </TableCell>
                <TableCell align="center">
                  <Tooltip
                    className={classes.OverflowCol}
                    title={exitDetail}
                    arrow
                    interactive
                  >
                    <div>{exitDetail}</div>
                  </Tooltip>
                </TableCell>
                <TableCell align="center">
                  {ipLogMap[address?.ipAddress] && (
                    <React.Fragment>
                      <Link
                        target="_blank"
                        to={`/log/${encodeURIComponent(
                          ipLogMap[address?.ipAddress],
                        )}?fileName=${jobId}-${pid}`}
                      >
                        Log
                      </Link>
                      <br />
                      <a
                        href={`/worker/traceback?pid=${pid}&ip=${address?.ipAddress}`}
                        target="_blank"
                        title="Sample the current Python stack trace for this worker."
                        rel="noreferrer"
                      >
                        Stack&nbsp;Trace
                      </a>
                      <br />
                      <a
                        href={`/worker/cpu_profile?pid=${pid}&ip=${address?.ipAddress}&duration=5`}
                        target="_blank"
                        title="Profile the Python worker for 5 seconds (default) and display a flame graph."
                        rel="noreferrer"
                      >
                        Flame&nbsp;Graph
                      </a>
                      <br />
                    </React.Fragment>
                  )}
                </TableCell>
              </ExpandableTableRow>
            ),
          )}
        </TableBody>
      </Table>
    </React.Fragment>
  );
};

export default ActorTable;
