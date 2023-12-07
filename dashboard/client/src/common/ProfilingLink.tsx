import {
  Button,
  Checkbox,
  Dialog,
  DialogContent,
  DialogTitle,
  FormControlLabel,
  InputLabel,
  Link,
  MenuItem,
  Select,
  TextField,
} from "@material-ui/core";
import React, { PropsWithChildren, useState } from "react";
import { ClassNameProps } from "./props";

type CpuProfilingLinkProps = PropsWithChildren<
  {
    pid: string | number | null | undefined;
    ip: string | null | undefined;
    type: string | null;
  } & ClassNameProps
>;

type TaskProfilingStackTraceProps = {
  taskId: string | null | undefined;
  attemptNumber: number;
  nodeId: string;
};

type MemoryProfilingProps = PropsWithChildren<
  {
    pid: string | number | null | undefined;
    ip: string | null | undefined;
    type: string | null;
  } & ClassNameProps
>;

type TaskMemoryProfilingProps = {
  taskId: string | null | undefined;
  attemptNumber: number;
  nodeId: string;
};

type MemoryProfilingButtonProps = {
  profilerUrl: string;
  type: string | null;
};

export const TaskCpuProfilingLink = ({
  taskId,
  attemptNumber,
  nodeId,
}: TaskProfilingStackTraceProps) => {
  if (!taskId) {
    return null;
  }
  return (
    <Link
      href={`task/cpu_profile?task_id=${taskId}&attempt_number=${attemptNumber}&node_id=${nodeId}`}
      target="_blank"
      title="Profile the Python worker for 5 seconds (default) and display a CPU flame graph."
      rel="noreferrer"
    >
      CPU&nbsp;Flame&nbsp;Graph
    </Link>
  );
};

export const TaskCpuStackTraceLink = ({
  taskId,
  attemptNumber,
  nodeId,
}: TaskProfilingStackTraceProps) => {
  if (!taskId) {
    return null;
  }
  return (
    <Link
      href={`task/traceback?task_id=${taskId}&attempt_number=${attemptNumber}&node_id=${nodeId}`}
      target="_blank"
      title="Sample the current Python stack trace for this worker."
      rel="noreferrer"
    >
      Stack&nbsp;Trace
    </Link>
  );
};

export const CpuStackTraceLink = ({
  pid,
  ip,
  type = "",
}: CpuProfilingLinkProps) => {
  if (!pid || !ip || typeof pid === "undefined" || typeof ip === "undefined") {
    return <div></div>;
  }
  return (
    <Link
      href={`worker/traceback?pid=${pid}&ip=${ip}&native=0`}
      target="_blank"
      title="Sample the current Python stack trace for this worker."
      rel="noreferrer"
    >
      Stack&nbsp;Trace{type ? ` (${type})` : ""}
    </Link>
  );
};

export const CpuProfilingLink = ({
  pid,
  ip,
  type = "",
}: CpuProfilingLinkProps) => {
  if (!pid || !ip) {
    return <div></div>;
  }

  return (
    <Link
      href={`worker/cpu_profile?pid=${pid}&ip=${ip}&duration=5&native=0`}
      target="_blank"
      title="Profile the Python worker for 5 seconds (default) and display a CPU flame graph."
      rel="noreferrer"
    >
      CPU&nbsp;Flame&nbsp;Graph{type ? ` (${type})` : ""}
    </Link>
  );
};

export const ProfilerButton = ({
  profilerUrl,
  type = "",
}: MemoryProfilingButtonProps) => {
  const [duration, setDuration] = useState<number | null>(5);
  const [native, setNative] = useState<boolean>(false);
  const [leaks, setLeaks] = useState<boolean>(false);
  const [open, setOpen] = useState<boolean>(false);
  const [format, setFormat] = useState("flamegraph");

  const handleOpen = () => {
    setOpen(true);
  };

  const handleClose = () => {
    setOpen(false);
  };

  return (
    <div>
      <Button variant="text" color="primary" onClick={handleOpen}>
        Profile&nbsp;Memory{type ? ` (${type})` : ""}
      </Button>

      <Dialog open={open} onClose={handleClose}>
        <DialogTitle>Memory Profiling Config</DialogTitle>
        <DialogContent>
          <InputLabel id="format-label">Format</InputLabel>
          <Select
            labelId="format-label"
            id="format"
            value={format}
            onChange={(e) => setFormat(e.target.value as string)}
            fullWidth
          >
            <MenuItem value="flamegraph">Flamegraph</MenuItem>
            <MenuItem value="table">Table</MenuItem>
          </Select>
          <br />
          <InputLabel id="format-label">Duration</InputLabel>
          <TextField
            label="Duration"
            type="number"
            value={duration !== null ? duration : ""}
            onChange={(e) => setDuration(parseInt(e.target.value, 10))}
            required
          />
          <br />
          <FormControlLabel
            control={
              <Checkbox
                checked={native}
                onChange={(e) => setNative(e.target.checked)}
              />
            }
            label="Native"
          />
          <br />
          <FormControlLabel
            control={
              <Checkbox
                checked={leaks}
                onChange={(e) => setLeaks(e.target.checked)}
              />
            }
            label="Leaks"
          />
        </DialogContent>
        <div
          style={{
            padding: "16px",
            display: "flex",
            justifyContent: "flex-end",
          }}
        >
          <Button color="primary" variant="text" onClick={handleClose}>
            <Link
              href={`${profilerUrl}&format=${format}&duration=${duration}&native=${
                native ? "1" : "0"
              }&leaks=${leaks ? "1" : "0"}`}
              rel="noreferrer"
              target="_blank"
            >
              Generate&nbsp;Report{type ? ` (${type})` : ""}
            </Link>
          </Button>
          <Button onClick={handleClose} color="primary" variant="text">
            Cancel
          </Button>
        </div>
      </Dialog>
    </div>
  );
};

export const MemoryProfilingButton = ({
  pid,
  ip,
  type = "",
}: MemoryProfilingProps) => {
  if (!pid || !ip) {
    return <div></div>;
  }
  const profilerUrl = `worker/memory_profile?pid=${pid}&ip=${ip}`;

  return <ProfilerButton profilerUrl={profilerUrl} type={type} />;
};

export const TaskMemoryProfilingButton = ({
  taskId,
  attemptNumber,
  nodeId,
}: TaskMemoryProfilingProps) => {
  if (!taskId) {
    return null;
  }
  const profilerUrl = `task/memory_profile?task_id=${taskId}&attempt_number=${attemptNumber}&node_id=${nodeId}`;

  return <ProfilerButton profilerUrl={profilerUrl} type="" />;
};
