import {
  InputAdornment,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TextField,
  TextFieldProps,
} from "@material-ui/core";
import { orange } from "@material-ui/core/colors";
import { SearchOutlined } from "@material-ui/icons";
import Autocomplete from "@material-ui/lab/Autocomplete";
import Pagination from "@material-ui/lab/Pagination";
import React, { useContext, useState } from "react";
import { Link } from "react-router-dom";
import { GlobalContext } from "../App";
import { Actor } from "../type/actor";
import { Worker } from "../type/worker";
import { longTextCut } from "../util/func";
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
  const actorList = Object.values(actors || {})
    .map((e) => ({
      ...e,
      functionDesc: Object.values(
        e.taskSpec?.functionDescriptor?.javaFunctionDescriptor ||
          e.taskSpec?.functionDescriptor?.pythonFunctionDescriptor ||
          {},
      ).join(" "),
    }))
    .filter(filterFunc);
  const list = actorList.slice((pageNo - 1) * pageSize, pageNo * pageSize);

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
          style={{ margin: 8, width: 200 }}
          label="Task Func Desc"
          size="small"
          InputProps={{
            onChange: ({ target: { value } }) => {
              changeFilter("functionDesc", value.trim());
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
          InputProps={{
            onChange: ({ target: { value } }) => {
              setPageSize(Math.min(Number(value), 500) || 10);
            },
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
              "ID(Num Restarts)",
              "Name",
              "Task Func Desc",
              "Job Id",
              "Pid",
              "IP",
              "Port",
              "State",
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
              functionDesc,
              jobId,
              pid,
              address,
              state,
              name,
              numRestarts,
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
                <TableCell
                  align="center"
                  style={{
                    color: Number(numRestarts) > 0 ? orange[500] : "inherit",
                  }}
                >
                  {actorId}({numRestarts})
                </TableCell>
                <TableCell align="center">{name}</TableCell>
                <TableCell align="center">
                  {longTextCut(functionDesc, 60)}
                </TableCell>
                <TableCell align="center">{jobId}</TableCell>
                <TableCell align="center">{pid}</TableCell>
                <TableCell align="center">{address?.ipAddress}</TableCell>
                <TableCell align="center">{address?.port}</TableCell>
                <TableCell align="center">
                  <StatusChip type="actor" status={state} />
                </TableCell>
                <TableCell align="center">
                  {ipLogMap[address?.ipAddress] && (
                    <Link
                      target="_blank"
                      to={`/log/${encodeURIComponent(
                        ipLogMap[address?.ipAddress],
                      )}?fileName=${jobId}-${pid}`}
                    >
                      Log
                    </Link>
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
