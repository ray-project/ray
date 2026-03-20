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
  Typography,
} from "@mui/material";
import React, { PropsWithChildren, useState } from "react";
import { HelpInfo } from "../components/Tooltip";
import { ClassNameProps } from "./props";

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
  nodeId,
  type = "",
}: CpuProfilingLinkProps) => {
  if (
    !pid ||
    !nodeId ||
    typeof pid === "undefined" ||
    typeof nodeId === "undefined"
  ) {
    return <div></div>;
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
  if (!pid || !nodeId) {
    return <div></div>;
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

type CpuProfilingButtonProps = {
  profilerUrl: string;
  type?: string | null;
};

export const CpuProfilerButton = ({
  profilerUrl,
  type,
}: CpuProfilingButtonProps) => {
  const [duration, setDuration] = useState(5);
  const [format, setFormat] = useState("flamegraph");
  const [native, setNative] = useState(false);
  const [rate, setRate] = useState(100);
  const [gil, setGil] = useState(false);
  const [idle, setIdle] = useState(false);
  const [nonblocking, setNonblocking] = useState(false);
  const [open, setOpen] = useState(false);
  const [perfettoConfirmOpen, setPerfettoConfirmOpen] = useState(false);

  const handleOpen = () => {
    setOpen(true);
  };

  const handleClose = () => {
    setOpen(false);
  };

  const buildProfileUrl = () => {
    return (
      `${profilerUrl}&format=${format}&duration=${duration}` +
      `&native=${native ? "1" : "0"}&rate=${rate}` +
      `&gil=${gil ? "1" : "0"}&idle=${idle ? "1" : "0"}&nonblocking=${nonblocking ? "1" : "0"}`
    );
  };

  const getBasePath = () => {
    return window.location.pathname.replace(/\/$/, "");
  };

  const openInSpeedscope = () => {
    const profileUrl = buildProfileUrl();
    const basePath = getBasePath();
    const absoluteUrl = `${window.location.origin}${basePath}/${profileUrl}`;
    const speedscopeUrl = `${basePath}/speedscope-1.5.3/index.html#profileURL=${encodeURIComponent(absoluteUrl)}`;
    window.open(speedscopeUrl, "_blank");
    handleClose();
  };

  const openInTraceViewer = () => {
    const profileUrl = buildProfileUrl();
    const basePath = getBasePath();
    const absoluteUrl = `${window.location.origin}${basePath}/${profileUrl}`;
    const traceViewerUrl = `${basePath}/trace-viewer/index.html#traceURL=${encodeURIComponent(absoluteUrl)}`;
    window.open(traceViewerUrl, "_blank");
    handleClose();
  };

  const openInPerfetto = async () => {
    const profileUrl = buildProfileUrl();
    const basePath = getBasePath();
    const absoluteUrl = `${window.location.origin}${basePath}/${profileUrl}`;

    // Fetch trace data from dashboard API
    const response = await fetch(absoluteUrl);
    if (!response.ok) {
      console.error("Failed to fetch profile data");
      return;
    }
    const buffer = await response.arrayBuffer();

    // Open Perfetto UI
    const PERFETTO_UI_URL = "https://ui.perfetto.dev";
    const perfettoWindow = window.open(PERFETTO_UI_URL);
    if (!perfettoWindow) {
      alert("Popup blocked. Please allow popups for this site.");
      return;
    }

    // Wait for Perfetto to be ready (PING/PONG handshake)
    const onPerfettoReady = (event: MessageEvent) => {
      if (event.data !== "PONG") return;
      if (event.origin !== PERFETTO_UI_URL) return;

      window.removeEventListener("message", onPerfettoReady);

      // Send trace data to Perfetto
      perfettoWindow.postMessage(
        {
          perfetto: {
            buffer: buffer,
            title: "Ray CPU Profile",
            fileName: "profile.json",
          },
        },
        PERFETTO_UI_URL,
      );
    };

    window.addEventListener("message", onPerfettoReady);

    // Send PING to initiate handshake (with retry)
    const sendPing = () => {
      perfettoWindow.postMessage("PING", PERFETTO_UI_URL);
    };

    const pingInterval = setInterval(sendPing, 100);
    setTimeout(() => clearInterval(pingInterval), 10000);

    setPerfettoConfirmOpen(false);
    handleClose();
  };

  return (
    <React.Fragment>
      <Link
        onClick={handleOpen}
        aria-label="CPU Profiling"
        sx={{ cursor: "pointer" }}
      >
        CPU&nbsp;Profiling{type ? ` (${type})` : ""}
      </Link>

      <Dialog open={open} onClose={handleClose}>
        <DialogTitle>CPU Profiling Config</DialogTitle>
        <DialogContent>
          <InputLabel id="cpu-format-label">Format</InputLabel>
          <Select
            labelId="cpu-format-label"
            id="cpu-format"
            value={format}
            aria-label={format}
            onChange={(e) => setFormat(e.target.value as string)}
            fullWidth
            style={{ marginBottom: "12px" }}
          >
            <MenuItem value="flamegraph">Flamegraph (SVG)</MenuItem>
            <MenuItem value="chrometrace">Chrome Trace (Timeline)</MenuItem>
          </Select>
          <TextField
            label="Duration (seconds)"
            type="number"
            value={duration !== null ? duration : ""}
            onChange={(e) => setDuration(parseInt(e.target.value, 10))}
            inputProps={{ min: 1, max: 60 }}
            required
            fullWidth
            style={{ marginBottom: "12px" }}
          />
          <TextField
            label="Sampling Rate (Hz)"
            type="number"
            value={rate !== null ? rate : ""}
            onChange={(e) => setRate(parseInt(e.target.value, 10))}
            inputProps={{ min: 1, max: 1000 }}
            fullWidth
            style={{ marginBottom: "12px" }}
          />
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
                    Track native (C/C++) stack frames. Only available on Linux.
                    Refer to py-spy documentation for more details.
                  </Typography>
                </HelpInfo>
              </div>
            }
          />
          <br />
          <FormControlLabel
            control={
              <Checkbox
                checked={gil}
                onChange={(e) => setGil(e.target.checked)}
              />
            }
            label={
              <div style={{ display: "flex", alignItems: "center" }}>
                <span style={{ marginRight: "4px" }}>GIL Only</span>
                <HelpInfo>
                  <Typography>
                    Only include traces that are holding on to the GIL (Global
                    Interpreter Lock). Useful for identifying Python code that
                    is actively executing.
                  </Typography>
                </HelpInfo>
              </div>
            }
          />
          <br />
          <FormControlLabel
            control={
              <Checkbox
                checked={idle}
                onChange={(e) => setIdle(e.target.checked)}
              />
            }
            label={
              <div style={{ display: "flex", alignItems: "center" }}>
                <span style={{ marginRight: "4px" }}>Include Idle</span>
                <HelpInfo>
                  <Typography>
                    Include stack traces for idle threads (e.g., threads in
                    time.sleep() or waiting on I/O). Useful for understanding
                    the full picture of thread activity.
                  </Typography>
                </HelpInfo>
              </div>
            }
          />
          <br />
          <FormControlLabel
            control={
              <Checkbox
                checked={nonblocking}
                onChange={(e) => setNonblocking(e.target.checked)}
              />
            }
            label={
              <div style={{ display: "flex", alignItems: "center" }}>
                <span style={{ marginRight: "4px" }}>Non-blocking</span>
                <HelpInfo>
                  <Typography>
                    Don't pause the Python process when collecting samples.
                    Reduces performance impact but may lead to less accurate
                    results.
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
            sx={{ textTransform: "capitalize", color: "#5F6469" }}
          >
            Cancel
          </Button>
          {format === "chrometrace" ? (
            <React.Fragment>
              <Button
                color="primary"
                variant="text"
                onClick={openInSpeedscope}
                style={{ textTransform: "capitalize" }}
              >
                Open&nbsp;in&nbsp;Speedscope
              </Button>
              <Button
                color="primary"
                variant="text"
                onClick={openInTraceViewer}
                style={{ textTransform: "capitalize" }}
              >
                Open&nbsp;in&nbsp;Trace&nbsp;Viewer
              </Button>
              <Button
                color="primary"
                variant="text"
                onClick={() => setPerfettoConfirmOpen(true)}
                style={{ textTransform: "capitalize" }}
              >
                Open&nbsp;in&nbsp;Perfetto
              </Button>
            </React.Fragment>
          ) : null}
          <Button
            color="primary"
            variant="text"
            onClick={handleClose}
            style={{ textTransform: "capitalize" }}
          >
            <Link href={buildProfileUrl()} rel="noreferrer" target="_blank">
              {format === "chrometrace" ? "Download" : "Generate\u00A0report"}
            </Link>
          </Button>
        </Box>
      </Dialog>

      <Dialog
        open={perfettoConfirmOpen}
        onClose={() => setPerfettoConfirmOpen(false)}
      >
        <DialogTitle>Open in Perfetto UI</DialogTitle>
        <DialogContent>
          <Typography>
            This will open <strong>ui.perfetto.dev</strong> (hosted by Google)
            and send your CPU profile data to that site.
          </Typography>
          <Typography style={{ marginTop: "8px" }}>
            According to Perfetto's documentation, the UI is client-only and
            trace data stays in your browser. However, you are trusting
            third-party JavaScript.
          </Typography>
          <Typography style={{ marginTop: "8px", fontWeight: "bold" }}>
            Do you want to continue?
          </Typography>
        </DialogContent>
        <Box
          sx={{ padding: "12px", display: "flex", justifyContent: "flex-end" }}
        >
          <Button
            onClick={() => setPerfettoConfirmOpen(false)}
            variant="text"
            sx={{ textTransform: "capitalize", color: "#5F6469" }}
          >
            Cancel
          </Button>
          <Button
            color="primary"
            variant="text"
            onClick={openInPerfetto}
            style={{ textTransform: "capitalize" }}
          >
            Open Perfetto
          </Button>
        </Box>
      </Dialog>
    </React.Fragment>
  );
};

export const CpuProfilingButton = ({
  pid,
  nodeId,
  type = "",
}: CpuProfilingLinkProps) => {
  if (!pid || !nodeId) {
    return <div></div>;
  }
  const profilerUrl = `worker/cpu_profile?pid=${pid}&node_id=${nodeId}`;

  return <CpuProfilerButton profilerUrl={profilerUrl} type={type} />;
};

export const TaskCpuProfilingButton = ({
  taskId,
  attemptNumber,
  nodeId,
}: TaskProfilingStackTraceProps) => {
  if (!taskId) {
    return null;
  }
  const profilerUrl = `task/cpu_profile?task_id=${taskId}&attempt_number=${attemptNumber}&node_id=${nodeId}`;

  return <CpuProfilerButton profilerUrl={profilerUrl} />;
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
    <React.Fragment>
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
    </React.Fragment>
  );
};

export const MemoryProfilingButton = ({
  pid,
  nodeId,
  type = "",
}: MemoryProfilingProps) => {
  if (!pid || !nodeId) {
    return <div></div>;
  }
  const profilerUrl = `memory_profile?pid=${pid}&node_id=${nodeId}`;

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
  const profilerUrl = `memory_profile?task_id=${taskId}&attempt_number=${attemptNumber}&node_id=${nodeId}`;

  return <ProfilerButton profilerUrl={profilerUrl} />;
};
