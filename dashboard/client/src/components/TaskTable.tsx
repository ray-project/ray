import {
  Box,
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
import Autocomplete from "@material-ui/lab/Autocomplete";
import Pagination from "@material-ui/lab/Pagination";
import React, { useState } from "react";
import { DurationText } from "../common/DurationText";
import rowStyles from "../common/RowStyles";
import { Task } from "../type/task";
import { useFilter } from "../util/hook";
import StateCounter from "./StatesCounter";
import { StatusChip } from "./StatusChip";

const TaskTable = ({
  tasks = [],
  jobId = null,
}: {
  tasks: Task[];
  jobId?: string | null;
}) => {
  const [pageNo, setPageNo] = useState(1);
  const { changeFilter, filterFunc } = useFilter();
  const [pageSize, setPageSize] = useState(10);
  const taskList = tasks.filter(filterFunc);
  const list = taskList.slice((pageNo - 1) * pageSize, pageNo * pageSize);
  const classes = rowStyles();

  const columns = [
    { label: "ID" },
    { label: "Name" },
    { label: "Job Id" },
    { label: "State" },
    { label: "Duration" },
    { label: "Function or Class Name" },
    { label: "Node Id" },
    { label: "Actor_id" },
    { label: "Type" },
    { label: "Required Resources" },
  ];

  return (
    <div>
      <div style={{ flex: 1, display: "flex", alignItems: "center" }}>
        <Autocomplete
          style={{ margin: 8, width: 120 }}
          options={Array.from(new Set(tasks.map((e) => e.task_id)))}
          onInputChange={(_: any, value: string) => {
            changeFilter("task_id", value.trim());
          }}
          renderInput={(params: TextFieldProps) => (
            <TextField {...params} label="Task ID" />
          )}
        />
        <Autocomplete
          style={{ margin: 8, width: 120 }}
          options={Array.from(new Set(tasks.map((e) => e.state)))}
          onInputChange={(_: any, value: string) => {
            changeFilter("state", value.trim());
          }}
          renderInput={(params: TextFieldProps) => (
            <TextField {...params} label="State" />
          )}
        />
        <Autocomplete
          style={{ margin: 8, width: 150 }}
          defaultValue={jobId}
          options={Array.from(new Set(tasks.map((e) => e.job_id)))}
          onInputChange={(_: any, value: string) => {
            changeFilter("job_id", value.trim());
          }}
          renderInput={(params: TextFieldProps) => (
            <TextField {...params} label="Job Id" />
          )}
        />
        <Autocomplete
          style={{ margin: 8, width: 150 }}
          options={Array.from(new Set(tasks.map((e) => e.name)))}
          onInputChange={(_: any, value: string) => {
            changeFilter("name", value.trim());
          }}
          renderInput={(params: TextFieldProps) => (
            <TextField {...params} label="Name" />
          )}
        />
        <Autocomplete
          style={{ margin: 8, width: 150 }}
          options={Array.from(new Set(tasks.map((e) => e.func_or_class_name)))}
          onInputChange={(_: any, value: string) => {
            changeFilter("func_or_class_name", value.trim());
          }}
          renderInput={(params: TextFieldProps) => (
            <TextField {...params} label="Function or Class Name" />
          )}
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
            count={Math.ceil(taskList.length / pageSize)}
          />
        </div>
        <div>
          <StateCounter type="task" list={taskList} />
        </div>
      </div>
      <Table>
        <TableHead>
          <TableRow>
            {columns.map(({ label }) => (
              <TableCell align="center" key={label}>
                <Box display="flex" justifyContent="center" alignItems="center">
                  {label}
                </Box>
              </TableCell>
            ))}
          </TableRow>
        </TableHead>
        <TableBody>
          {list.map(
            ({
              task_id,
              name,
              job_id,
              state,
              func_or_class_name,
              node_id,
              actor_id,
              type,
              required_resources,
              events,
              start_time_ms,
              end_time_ms
            }) => (
              <TableRow>
                <TableCell align="center">
                  <Tooltip
                    className={classes.idCol}
                    title={task_id}
                    arrow
                    interactive
                  >
                    <div>{task_id}</div>
                  </Tooltip>
                </TableCell>
                <TableCell align="center">{name ? name : "-"}</TableCell>
                <TableCell align="center">{job_id}</TableCell>
                <TableCell align="center">
                  <StatusChip type="actor" status={state} />
                </TableCell>
                <TableCell align="center">
                  {start_time_ms && start_time_ms > 0 ? (
                    <DurationText startTime={start_time_ms} endTime={end_time_ms} />
                  ) : (
                    "-"
                  )}
                </TableCell>
                <TableCell align="center">{func_or_class_name}</TableCell>
                <TableCell align="center">{node_id ? node_id : "-"}</TableCell>
                <TableCell align="center">
                  {actor_id ? actor_id : "-"}
                </TableCell>
                <TableCell align="center">{type}</TableCell>
                <TableCell align="center">
                  <Tooltip
                    className={classes.OverflowCol}
                    title={Object.entries(required_resources || {}).map(
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
                      {Object.entries(required_resources || {})
                        .map(([key, val]) => `${key}: ${val}`)
                        .join(", ")}
                    </div>
                  </Tooltip>
                </TableCell>
              </TableRow>
            ),
          )}
        </TableBody>
      </Table>
    </div>
  );
};

export default TaskTable;
