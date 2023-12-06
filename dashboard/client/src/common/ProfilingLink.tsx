import { Button, Link, TextField } from "@material-ui/core";
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

export const CpuProfilingLink = ({
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

export const CpuStackTraceLink = ({
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

export const MemoryProfilingButton = ({
  pid,
  ip,
  type = "",
}: MemoryProfilingProps) => {
  const [duration, setDuration] = useState<number | null>(10);
  const [fillDuration, setFillDuration] = useState<boolean>(false);

  const handleButtonClick = () => {
    setFillDuration(true);
  };
  const handleInputSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    setFillDuration(false);
  };

  if (!pid || !ip) {
    return <div></div>;
  }
  return (
    <div>
      {!fillDuration ? (
        <Button
          variant="text"
          onClick={handleButtonClick}
          style={{ color: "blue" }}
        >
          Profile&nbsp;Memory{type ? ` (${type})` : ""}
        </Button>
      ) : (
        <form onSubmit={handleInputSubmit}>
          <TextField
            label="Duration"
            type="number"
            value={duration !== null ? duration : ""}
            onChange={(e) => setDuration(parseInt(e.target.value, 10))}
            required
          />
          <Button variant="text">
            <Link
              href={`worker/memory_profile?pid=${pid}&ip=${ip}&duration=${duration}&native=0&leaks=1`}
              rel="noreferrer"
              target="_blank"
            >
              Show&nbsp;Flame&nbsp;Graph{type ? ` (${type})` : ""}
            </Link>
          </Button>
        </form>
      )}
    </div>
  );
};

export const TaskMemoryProfilingButton = ({
  taskId,
  attemptNumber,
  nodeId,
}: TaskMemoryProfilingProps) => {
  const [duration, setDuration] = useState<number | null>(10);
  const [fillDuration, setFillDuration] = useState<boolean>(false);
  const handleButtonClick = () => {
    setFillDuration(true);
  };
  const handleInputSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    setFillDuration(false);
  };

  if (!taskId) {
    return null;
  }
  return (
    <div>
      {!fillDuration ? (
        <Button
          variant="text"
          onClick={handleButtonClick}
          style={{ color: "blue" }}
        >
          Profile&nbsp;Memory
        </Button>
      ) : (
        <form onSubmit={handleInputSubmit}>
          <TextField
            label="Duration"
            type="number"
            value={duration !== null ? duration : ""}
            onChange={(e) => setDuration(parseInt(e.target.value, 10))}
            required
          />
          <Button variant="text">
            <Link
              href={`task/memory_profile?task_id=${taskId}&duration=${duration}&attempt_number=${attemptNumber}&node_id=${nodeId}`}
              rel="noreferrer"
              target="_blank"
            >
              Show&nbsp;Flame&nbsp;Graph
            </Link>
          </Button>
        </form>
      )}
    </div>
  );
};
