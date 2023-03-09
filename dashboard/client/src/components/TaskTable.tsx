import {
  Box,
  createStyles,
  InputAdornment,
  makeStyles,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TextField,
  TextFieldProps,
  Tooltip,
  Typography,
} from "@material-ui/core";
import Autocomplete from "@material-ui/lab/Autocomplete";
import Pagination from "@material-ui/lab/Pagination";
import React, { useContext, useState } from "react";
import { Link } from "react-router-dom";
import { GlobalContext } from "../App";
import DialogWithTitle from "../common/DialogWithTitle";
import { DurationText } from "../common/DurationText";
import rowStyles from "../common/RowStyles";
import { Task } from "../type/task";
import { useFilter } from "../util/hook";
import StateCounter from "./StatesCounter";
import { StatusChip } from "./StatusChip";
import { HelpInfo } from "./Tooltip";

export type TaskTableProps = {
  tasks: Task[];
  jobId?: string;
  filterToTaskId?: string;
  onFilterChange?: () => void;
  newIA?: boolean;
  actorId?: string;
};

const TaskTable = ({
  tasks = [],
  jobId,
  filterToTaskId,
  onFilterChange,
  newIA = false,
  actorId,
}: TaskTableProps) => {
  const [pageNo, setPageNo] = useState(1);
  const { changeFilter, filterFunc } = useFilter<keyof Task>({
    overrideFilters:
      filterToTaskId !== undefined
        ? [{ key: "task_id", val: filterToTaskId }]
        : undefined,
    onFilterChange,
  });
  const [taskIdFilterValue, setTaskIdFilterValue] = useState(filterToTaskId);
  const [pageSize, setPageSize] = useState(10);
  const taskList = tasks.filter(filterFunc);
  const list = taskList.slice((pageNo - 1) * pageSize, pageNo * pageSize);
  const classes = rowStyles();

  const columns = [
    { label: "ID" },
    { label: "Name" },
    { label: "Job Id" },
    { label: "State" },
    {
      label: "Actions",
      helpInfo: (
        <Typography>
          A list of actions performable on this task.
          <br />
          - Log: view log messages of the worker that ran this task. You can
          only view all the logs of the worker and a worker can run multiple
          tasks.
          <br />- Error: For tasks that have failed, show a stack trace for the
          faiure.
        </Typography>
      ),
    },
    { label: "Duration" },
    { label: "Function or Class Name" },
    { label: "Node Id" },
    { label: "Actor_id" },
    { label: "Worker_id" },
    { label: "Type" },
    { label: "Placement Group Id" },
    { label: "Required Resources" },
  ];

  return (
    <div>
      <div style={{ flex: 1, display: "flex", alignItems: "center" }}>
        <Autocomplete
          value={filterToTaskId ?? taskIdFilterValue}
          inputValue={filterToTaskId ?? taskIdFilterValue}
          style={{ margin: 8, width: 120 }}
          options={Array.from(new Set(tasks.map((e) => e.task_id)))}
          onInputChange={(_: any, value: string) => {
            changeFilter("task_id", value.trim());
            setTaskIdFilterValue(value);
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
          defaultValue={filterToTaskId === undefined ? jobId : undefined}
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
          defaultValue={filterToTaskId === undefined ? actorId : undefined}
          options={Array.from(
            new Set(tasks.map((e) => (e.actor_id ? e.actor_id : ""))),
          )}
          onInputChange={(_: any, value: string) => {
            changeFilter("actor_id", value.trim());
          }}
          renderInput={(params: TextFieldProps) => (
            <TextField {...params} label="Actor Id" />
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
      <div className={classes.tableContainer}>
        <Table>
          <TableHead>
            <TableRow>
              {columns.map(({ label, helpInfo }) => (
                <TableCell align="center" key={label}>
                  <Box
                    display="flex"
                    justifyContent="center"
                    alignItems="center"
                  >
                    {label}
                    {helpInfo && (
                      <HelpInfo className={classes.helpInfo}>
                        {helpInfo}
                      </HelpInfo>
                    )}
                  </Box>
                </TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {list.map((task) => {
              const {
                task_id,
                name,
                job_id,
                state,
                func_or_class_name,
                node_id,
                actor_id,
                placement_group_id,
                type,
                required_resources,
                start_time_ms,
                end_time_ms,
                worker_id,
              } = task;
              return (
                <TableRow key={task_id}>
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
                    <StatusChip type="task" status={state} />
                  </TableCell>
                  <TableCell align="center">
                    <TaskTableActions task={task} newIA={newIA} />
                  </TableCell>
                  <TableCell align="center">
                    {start_time_ms && start_time_ms > 0 ? (
                      <DurationText
                        startTime={start_time_ms}
                        endTime={end_time_ms}
                      />
                    ) : (
                      "-"
                    )}
                  </TableCell>
                  <TableCell align="center">{func_or_class_name}</TableCell>
                  <TableCell align="center">
                    <Tooltip
                      className={classes.idCol}
                      title={node_id ? node_id : "-"}
                      arrow
                      interactive
                    >
                      <div>{node_id ? node_id : "-"}</div>
                    </Tooltip>
                  </TableCell>
                  <TableCell align="center">
                    <Tooltip
                      className={classes.idCol}
                      title={actor_id ? actor_id : "-"}
                      arrow
                      interactive
                    >
                      <div>{actor_id ? actor_id : "-"}</div>
                    </Tooltip>
                  </TableCell>
                  <TableCell align="center">
                    <Tooltip
                      className={classes.idCol}
                      title={worker_id ? worker_id : "-"}
                      arrow
                      interactive
                    >
                      <div>{worker_id ? worker_id : "-"}</div>
                    </Tooltip>
                  </TableCell>
                  <TableCell align="center">{type}</TableCell>
                  <TableCell align="center">
                    <Tooltip
                      className={classes.idCol}
                      title={placement_group_id ? placement_group_id : "-"}
                      arrow
                      interactive
                    >
                      <div>{placement_group_id ? placement_group_id : "-"}</div>
                    </Tooltip>
                  </TableCell>
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
              );
            })}
          </TableBody>
        </Table>
      </div>
    </div>
  );
};

export default TaskTable;

const useTaskTableActionsStyles = makeStyles(() =>
  createStyles({
    errorDetails: {
      whiteSpace: "pre",
    },
    link: {
      border: "none",
      cursor: "pointer",
      color: "#036DCF",
      textDecoration: "underline",
      background: "none",
    },
  }),
);

type TaskTableActionsProps = {
  newIA?: boolean;
  task: Task;
};

const TaskTableActions = ({ task, newIA = false }: TaskTableActionsProps) => {
  const classes = useTaskTableActionsStyles();
  const { ipLogMap } = useContext(GlobalContext);
  const [showErrorDetailsDialog, setShowErrorDetailsDialog] = useState(false);

  const handleErrorClick = () => {
    setShowErrorDetailsDialog(true);
  };

  const executeEvent = task.profiling_data?.events?.find(
    ({ event_name }) => event_name === "task:execute",
  );
  const errorDetails =
    executeEvent?.extra_data?.traceback && executeEvent?.extra_data?.type
      ? `${executeEvent?.extra_data?.type}\n${executeEvent?.extra_data?.traceback}`
      : undefined;

  return (
    <React.Fragment>
      {task?.profiling_data?.node_ip_address &&
        ipLogMap[task?.profiling_data?.node_ip_address] &&
        task.worker_id &&
        task.job_id && (
          <React.Fragment>
            <Link
              target="_blank"
              to={
                newIA
                  ? `/new/logs/${encodeURIComponent(
                      ipLogMap[task.profiling_data.node_ip_address],
                    )}?fileName=worker-${task.worker_id}`
                  : `/log/${encodeURIComponent(
                      ipLogMap[task.profiling_data.node_ip_address],
                    )}?fileName=worker-${task.worker_id}`
              }
            >
              Log
            </Link>
            <br />
          </React.Fragment>
        )}
      {errorDetails && (
        <button className={classes.link} onClick={handleErrorClick}>
          Error
        </button>
      )}
      {showErrorDetailsDialog && errorDetails && (
        <DialogWithTitle
          title="Error details"
          handleClose={() => {
            setShowErrorDetailsDialog(false);
          }}
        >
          <div className={classes.errorDetails}>{errorDetails}</div>
        </DialogWithTitle>
      )}
    </React.Fragment>
  );
};
