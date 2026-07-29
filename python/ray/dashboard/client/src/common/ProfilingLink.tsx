import {
  Box,
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
  Tooltip,
  Typography,
} from "@mui/material";
import React, { PropsWithChildren, useEffect, useState } from "react";
import { HelpInfo } from "../components/Tooltip";
import { ClassNameProps } from "./props";

let cachedProfilingEnabled: boolean | null = null;
let fetchPromise: Promise<void> | null = null;

const fetchProfilingEnabled = (): Promise<void> => {
  if (cachedProfilingEnabled !== null) {
    return Promise.resolve();
  }
  if (!fetchPromise) {
    fetchPromise = fetch("/api/profiling_enabled")
      .then((res) => res.json())
      .then((data) => {
        cachedProfilingEnabled = data.data.profilingEnabled;
      })
      .catch(() => {
        cachedProfilingEnabled = false;
      });
  }
  return fetchPromise;
};

const useProfilingEnabled = () => {
  const [enabled, setEnabled] = useState(cachedProfilingEnabled ?? false);

  useEffect(() => {
    fetchProfilingEnabled().then(() =>
      setEnabled(cachedProfilingEnabled ?? false),
    );
  }, []);

  return enabled;
};

const PROFILING_DISABLED_TOOLTIP =
  "Profiling is disabled by default for security. " +
  "Set RAY_DASHBOARD_ENABLE_PROFILING=1 environment variable on the Ray head node to enable. " +
  "See https://docs.ray.io/en/latest/ray-observability/user-guides/profiling.html#enabling-dashboard-profiling";

const DisabledProfilingLabel = ({
  children,
}: {
  children: React.ReactNode;
}) => (
  <Tooltip title={PROFILING_DISABLED_TOOLTIP}>
    <Typography component="span" color="text.disabled">
      {children}
    </Typography>
  </Tooltip>
);

type CpuProfilingLinkProps = PropsWithChildren<
  {
    pid: string | number | null | undefined;
    nodeId: string | null | undefined;
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
    nodeId: string | null | undefined;
    type?: string | null;
  } & ClassNameProps
>;

type TaskMemoryProfilingProps = {
  taskId: string | null | undefined;
  attemptNumber: number;
  nodeId: string;
};

type MemoryProfilingButtonProps = {
  profilerUrl: string;
  type?: string | null;
};

export const TaskCpuProfilingLink = ({
  taskId,
  attemptNumber,
  nodeId,
}: TaskProfilingStackTraceProps) => {
  const profilingEnabled = useProfilingEnabled();
  if (!taskId) {
    return null;
  }
  if (!profilingEnabled) {
    return (
      <DisabledProfilingLabel>CPU&nbsp;Flame&nbsp;Graph</DisabledProfilingLabel>
    );
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
  const profilingEnabled = useProfilingEnabled();
  if (!taskId) {
    return null;
  }
  if (!profilingEnabled) {
    return <DisabledProfilingLabel>Stack&nbsp;Trace</DisabledProfilingLabel>;
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
  nodeId,
  type = "",
}: CpuProfilingLinkProps) => {
  const profilingEnabled = useProfilingEnabled();
  if (
    !pid ||
    !nodeId ||
    typeof pid === "undefined" ||
    typeof nodeId === "undefined"
  ) {
    return <div></div>;
  }
  if (!profilingEnabled) {
    return (
      <DisabledProfilingLabel>
        Stack&nbsp;Trace{type ? ` (${type})` : ""}
      </DisabledProfilingLabel>
    );
  }
  return (
    <Link
      href={`worker/traceback?pid=${pid}&node_id=${nodeId}&native=0`}
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
  nodeId,
  type = "",
}: CpuProfilingLinkProps) => {
  const profilingEnabled = useProfilingEnabled();
  if (!pid || !nodeId) {
    return <div></div>;
  }
  if (!profilingEnabled) {
    return (
      <DisabledProfilingLabel>
        CPU&nbsp;Flame&nbsp;Graph{type ? ` (${type})` : ""}
      </DisabledProfilingLabel>
    );
  }
  return (
    <Link
      href={`worker/cpu_profile?pid=${pid}&node_id=${nodeId}&duration=5&native=0`}
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
  type,
}: MemoryProfilingButtonProps) => {
  const [duration, setDuration] = useState(5);
  const [leaks, setLeaks] = useState(true);
  const [native, setNative] = useState(false);
  const [allocator, setAllocator] = useState(false);
  const [open, setOpen] = useState(false);
  const [format, setFormat] = useState("flamegraph");

  const handleOpen = () => {
    setOpen(true);
  };

  const handleClose = () => {
    setOpen(false);
  };

  return (
    <div>
      <Link
        onClick={handleOpen}
        aria-label="Memory Profiling"
        sx={{ cursor: "pointer" }}
      >
        Memory&nbsp;Profiling{type ? ` (${type})` : ""}
      </Link>

      <Dialog open={open} onClose={handleClose}>
        <DialogTitle>Memory Profiling Config</DialogTitle>
        <DialogContent>
          <InputLabel id="format-label">Format</InputLabel>
          <Select
            labelId="format-label"
            id="format"
            value={format}
            aria-label={format}
            onChange={(e) => setFormat(e.target.value as string)}
            fullWidth
            style={{ marginBottom: "12px" }}
          >
            <MenuItem value="flamegraph">Flamegraph</MenuItem>
            <MenuItem value="table">Table</MenuItem>
          </Select>
          <TextField
            label="Duration (seconds)"
            type="number"
            value={duration !== null ? duration : ""}
            onChange={(e) => setDuration(parseInt(e.target.value, 10))}
            required
          />
          <br />
          <FormControlLabel
            control={
              <Checkbox
                checked={leaks}
                onChange={(e) => setLeaks(e.target.checked)}
              />
            }
            label={
              <div style={{ display: "flex", alignItems: "center" }}>
                <span style={{ marginRight: "4px" }}>Leaks</span>
                <HelpInfo>
                  <Typography>
                    Enable memory leaks, instead of peak memory usage. Refer to
                    Memray documentation for more details.
                  </Typography>
                </HelpInfo>
              </div>
            }
          />
          <br />
          <FormControlLabel
            control={
              <Checkbox
                checked={native}
                onChange={(e) => setNative(e.target.checked)}
              />
            }
            label={
              <div style={{ display: "flex", alignItems: "center" }}>
                <span style={{ marginRight: "4px" }}>Native</span>
                <HelpInfo>
                  <Typography>
                    Track native (C/C++) stack frames. Refer to Memray
                    documentation for more details.
                  </Typography>
                </HelpInfo>
              </div>
            }
          />
          <br />
          <FormControlLabel
            control={
              <Checkbox
                checked={allocator}
                onChange={(e) => setAllocator(e.target.checked)}
              />
            }
            label={
              <div style={{ display: "flex", alignItems: "center" }}>
                <span style={{ marginRight: "4px" }}>
                  Python Allocator Tracing
                </span>
                <HelpInfo>
                  <Typography>
                    Record allocations made by the pymalloc allocator. Refer to
                    Memray documentation for more details.
                  </Typography>
                </HelpInfo>
              </div>
            }
          />
        </DialogContent>
        <Box
          sx={{ padding: "12px", display: "flex", justifyContent: "flex-end" }}
        >
          <Button
            onClick={handleClose}
            variant="text"
            sx={(theme) => ({
              textTransform: "capitalize",
              color: theme.palette.text.secondary,
            })}
          >
            Cancel
          </Button>
          <Button
            color="primary"
            variant="text"
            onClick={handleClose}
            style={{ textTransform: "capitalize" }}
          >
            <Link
              href={
                `${profilerUrl}&format=${format}&duration=${duration}` +
                `&leaks=${leaks ? "1" : "0"}` +
                `&native=${native ? "1" : "0"}` +
                `&trace_python_allocators=${allocator ? "1" : "0"}`
              }
              rel="noreferrer"
              target="_blank"
            >
              Generate&nbsp;report
            </Link>
          </Button>
        </Box>
      </Dialog>
    </div>
  );
};

export const MemoryProfilingButton = ({
  pid,
  nodeId,
  type = "",
}: MemoryProfilingProps) => {
  const profilingEnabled = useProfilingEnabled();
  if (!pid || !nodeId) {
    return <div></div>;
  }
  if (!profilingEnabled) {
    return (
      <DisabledProfilingLabel>
        Memory&nbsp;Profiling{type ? ` (${type})` : ""}
      </DisabledProfilingLabel>
    );
  }
  const profilerUrl = `memory_profile?pid=${pid}&node_id=${nodeId}`;

  return <ProfilerButton profilerUrl={profilerUrl} type={type} />;
};

export const TaskMemoryProfilingButton = ({
  taskId,
  attemptNumber,
  nodeId,
}: TaskMemoryProfilingProps) => {
  const profilingEnabled = useProfilingEnabled();
  if (!taskId) {
    return null;
  }
  if (!profilingEnabled) {
    return (
      <DisabledProfilingLabel>Memory&nbsp;Profiling</DisabledProfilingLabel>
    );
  }
  const profilerUrl = `memory_profile?task_id=${taskId}&attempt_number=${attemptNumber}&node_id=${nodeId}`;

  return <ProfilerButton profilerUrl={profilerUrl} />;
};
