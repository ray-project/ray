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
import { get } from "../service/requestHandlers";
import { ClassNameProps } from "./props";

// Cluster-wide defaults for the profiling parameters, configurable by the
// operator via RAY_DASHBOARD_PROFILING_*_DEFAULT env vars and returned by
// /api/profiling_enabled. The profiling dialogs seed their initial state from
// these so a user sees the same defaults the backend would apply.
export type ProfilingDefaults = {
  native: boolean;
  subprocesses: boolean;
  idle: boolean;
  leaks: boolean;
  tracePythonAllocators: boolean;
  cpuDuration: number;
  memoryDuration: number;
  // Upper bound the backend enforces on `duration` (operator-configurable via
  // RAY_DASHBOARD_PROFILING_MAX_DURATION_S). The lower bound is always 1.
  maxDuration: number;
  cpuFormat: string;
  memoryFormat: string;
  // Whether py-spy `--native` takes effect on this platform (Linux-only). Not a
  // configurable default -- reported by the backend so the CPU/stack-trace
  // dialogs can disable the Native checkbox off-Linux, where it is a silent
  // no-op. memray (memory) native is cross-platform and is unaffected.
  pyspyNativeSupported: boolean;
};

// Fallback used before /api/profiling_enabled resolves or if it fails. These
// mirror the backend's shipped defaults so behavior is unchanged offline.
export const DEFAULT_PROFILING_DEFAULTS: ProfilingDefaults = {
  native: false,
  subprocesses: false,
  idle: false,
  leaks: false,
  tracePythonAllocators: false,
  cpuDuration: 5,
  memoryDuration: 10,
  maxDuration: 60,
  cpuFormat: "flamegraph",
  memoryFormat: "flamegraph",
  // Assume supported until the backend says otherwise, so an unreachable
  // /api/profiling_enabled never spuriously disables the Native checkbox.
  pyspyNativeSupported: true,
};

let cachedProfilingEnabled: boolean | null = null;
let cachedProfilingDefaults: ProfilingDefaults | null = null;
let fetchPromise: Promise<void> | null = null;

// Exported for tests: reset the module-level cache so each test starts fresh
// and can control the mocked /api/profiling_enabled response independently.
export const _resetProfilingEnabledCache = () => {
  cachedProfilingEnabled = null;
  cachedProfilingDefaults = null;
  fetchPromise = null;
};

const fetchProfilingEnabled = (): Promise<void> => {
  if (cachedProfilingEnabled !== null) {
    return Promise.resolve();
  }
  if (!fetchPromise) {
    fetchPromise = get("/api/profiling_enabled")
      .then((res) => {
        cachedProfilingEnabled = res.data?.data?.profilingEnabled ?? false;
        // Merge onto the fallback so a partial/older payload still yields a
        // complete, well-typed defaults object.
        cachedProfilingDefaults = {
          ...DEFAULT_PROFILING_DEFAULTS,
          ...(res.data?.data?.profilingDefaults ?? {}),
        };
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

const useProfilingDefaults = (): ProfilingDefaults => {
  const [defaults, setDefaults] = useState(
    cachedProfilingDefaults ?? DEFAULT_PROFILING_DEFAULTS,
  );

  useEffect(() => {
    fetchProfilingEnabled().then(() =>
      setDefaults(cachedProfilingDefaults ?? DEFAULT_PROFILING_DEFAULTS),
    );
  }, []);

  return defaults;
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

// A single boolean profiling parameter rendered as a labelled checkbox.
type ProfilingFlag = {
  // Query-param name, e.g. "native". Used as the React key.
  key: string;
  label: string;
  help: string;
  initial: boolean;
  // When true, the checkbox is rendered read-only (e.g. py-spy native off-Linux).
  disabled: boolean;
};

// Optional numeric "duration" field.
type ProfilingDurationField = {
  initial: number;
  // Inclusive upper bound accepted by the backend (lower bound is always 1).
  max: number;
};

// Optional "format" select over a fixed set of allowed values.
type ProfilingFormatField = {
  initial: string;
  options: { value: string; label: string }[];
};

type ProfilingParamsDialogProps = {
  // Trigger link text (e.g. "Stack Trace") and dialog heading.
  label: React.ReactNode;
  dialogTitle: string;
  triggerTitle: string;
  submitLabel: string;
  duration?: ProfilingDurationField;
  format?: ProfilingFormatField;
  flags: ProfilingFlag[];
  // Build the final profiling URL from the values chosen in the dialog. The
  // `flags` map is keyed by each flag's `key`.
  buildUrl: (values: {
    duration: number;
    format: string;
    flags: Record<string, boolean>;
  }) => string;
};

// A reusable profiling dialog: a trigger link that opens a config dialog with an
// optional format select, an optional duration field, and a set of boolean
// flags, then links to the URL produced by `buildUrl`. All profiling actions
// (stack trace, CPU flame graph, memory profiling) render this same component
// with different fields so the UX stays consistent.
export const ProfilingParamsDialog = ({
  label,
  dialogTitle,
  triggerTitle,
  submitLabel,
  duration,
  format,
  flags,
  buildUrl,
}: ProfilingParamsDialogProps) => {
  const [open, setOpen] = useState(false);
  const [durationValue, setDurationValue] = useState(duration?.initial ?? 0);
  const [formatValue, setFormatValue] = useState(format?.initial ?? "");
  const [flagValues, setFlagValues] = useState<Record<string, boolean>>(() =>
    Object.fromEntries(flags.map((f) => [f.key, f.initial])),
  );

  // Reseed state from the latest props each time the dialog opens, so it always
  // reflects the current operator-configured defaults (which arrive
  // asynchronously from /api/profiling_enabled) rather than whatever was
  // captured at first mount.
  const handleOpen = () => {
    setDurationValue(duration?.initial ?? 0);
    setFormatValue(format?.initial ?? "");
    setFlagValues(Object.fromEntries(flags.map((f) => [f.key, f.initial])));
    setOpen(true);
  };
  const handleClose = () => setOpen(false);

  // Duration must be an integer in [1, duration.max] (matches the backend
  // validation, whose upper bound is operator-configurable). Only relevant when
  // the dialog shows a duration field.
  const durationInvalid =
    duration !== undefined &&
    (Number.isNaN(durationValue) ||
      durationValue < 1 ||
      durationValue > duration.max);

  return (
    <div>
      <Link
        onClick={handleOpen}
        aria-label={dialogTitle}
        sx={{ cursor: "pointer" }}
        title={triggerTitle}
      >
        {label}
      </Link>

      <Dialog open={open} onClose={handleClose}>
        <DialogTitle>{dialogTitle}</DialogTitle>
        <DialogContent>
          {format && (
            <React.Fragment>
              <InputLabel id="format-label">Format</InputLabel>
              <Select
                labelId="format-label"
                id="format"
                value={formatValue}
                aria-label={formatValue}
                onChange={(e) => setFormatValue(e.target.value as string)}
                fullWidth
                style={{ marginBottom: "12px" }}
              >
                {format.options.map((o) => (
                  <MenuItem key={o.value} value={o.value}>
                    {o.label}
                  </MenuItem>
                ))}
              </Select>
            </React.Fragment>
          )}
          {duration && (
            <React.Fragment>
              <TextField
                label="Duration (seconds)"
                type="number"
                value={Number.isNaN(durationValue) ? "" : durationValue}
                onChange={(e) => setDurationValue(parseInt(e.target.value, 10))}
                error={durationInvalid}
                helperText={
                  durationInvalid
                    ? `Duration must be between 1 and ${duration?.max}`
                    : ""
                }
                required
              />
              <br />
            </React.Fragment>
          )}
          {flags.map((flag) => (
            <React.Fragment key={flag.key}>
              <FormControlLabel
                control={
                  <Checkbox
                    checked={flagValues[flag.key]}
                    disabled={flag.disabled}
                    onChange={(e) =>
                      setFlagValues((prev) => ({
                        ...prev,
                        [flag.key]: e.target.checked,
                      }))
                    }
                  />
                }
                label={
                  <div style={{ display: "flex", alignItems: "center" }}>
                    <span style={{ marginRight: "4px" }}>{flag.label}</span>
                    <HelpInfo>
                      <Typography>{flag.help}</Typography>
                    </HelpInfo>
                  </div>
                }
              />
              <br />
            </React.Fragment>
          ))}
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
            // Render the button itself as the anchor rather than nesting a
            // <Link> inside it (nesting interactive elements is invalid HTML).
            component="a"
            href={buildUrl({
              duration: durationValue,
              format: formatValue,
              flags: flagValues,
            })}
            disabled={durationInvalid}
            rel="noreferrer"
            target="_blank"
          >
            {submitLabel}
          </Button>
        </Box>
      </Dialog>
    </div>
  );
};

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
  // Defaults to seed the dialog. Optional so the component can be rendered
  // standalone (e.g. in tests) without wiring up the profiling-defaults fetch.
  defaults?: ProfilingDefaults;
};

// CPU profiling accepts flamegraph/raw/speedscope (py-spy); memory accepts
// flamegraph/table (memray).
const CPU_FORMAT_OPTIONS = [
  { value: "flamegraph", label: "Flamegraph" },
  { value: "raw", label: "Raw" },
  { value: "speedscope", label: "Speedscope" },
];

const flag = (
  key: string,
  label: string,
  help: string,
  initial: boolean,
  disabled = false,
) => ({
  key,
  label,
  help,
  initial,
  disabled,
});

const NATIVE_HELP =
  "Include native (C/C++) stack frames. Adds significant profiling overhead " +
  "and is only supported on Linux.";
const SUBPROCESSES_HELP = "Also profile child processes of the target process.";
const IDLE_HELP = "Include off-CPU (sleeping) threads in the profile.";

// Serialize a set of boolean flags into `&name=1|0` query fragments.
const flagQuery = (flags: Record<string, boolean>): string =>
  Object.entries(flags)
    .map(([k, v]) => `&${k}=${v ? "1" : "0"}`)
    .join("");

const stackTraceFlags = (defaults: ProfilingDefaults) => [
  flag(
    "native",
    "Native",
    NATIVE_HELP,
    defaults.native && defaults.pyspyNativeSupported,
    !defaults.pyspyNativeSupported,
  ),
  flag(
    "subprocesses",
    "Subprocesses",
    SUBPROCESSES_HELP,
    defaults.subprocesses,
  ),
];

const cpuProfileFlags = (defaults: ProfilingDefaults) => [
  flag(
    "native",
    "Native",
    NATIVE_HELP,
    defaults.native && defaults.pyspyNativeSupported,
    !defaults.pyspyNativeSupported,
  ),
  flag("idle", "Idle", IDLE_HELP, defaults.idle),
  flag(
    "subprocesses",
    "Subprocesses",
    SUBPROCESSES_HELP,
    defaults.subprocesses,
  ),
];

export const TaskCpuProfilingLink = ({
  taskId,
  attemptNumber,
  nodeId,
}: TaskProfilingStackTraceProps) => {
  const profilingEnabled = useProfilingEnabled();
  const defaults = useProfilingDefaults();
  if (!taskId) {
    return null;
  }
  if (!profilingEnabled) {
    return (
      <DisabledProfilingLabel>CPU&nbsp;Flame&nbsp;Graph</DisabledProfilingLabel>
    );
  }
  return (
    <ProfilingParamsDialog
      label={<React.Fragment>CPU&nbsp;Flame&nbsp;Graph</React.Fragment>}
      dialogTitle="CPU Profiling Config"
      triggerTitle="Profile the Python worker and display a CPU flame graph."
      submitLabel="Generate report"
      duration={{ initial: defaults.cpuDuration, max: defaults.maxDuration }}
      format={{ initial: defaults.cpuFormat, options: CPU_FORMAT_OPTIONS }}
      flags={cpuProfileFlags(defaults)}
      buildUrl={({ duration, format, flags }) =>
        `task/cpu_profile?task_id=${taskId}&attempt_number=${attemptNumber}` +
        `&node_id=${nodeId}&duration=${duration}&format=${format}` +
        flagQuery(flags)
      }
    />
  );
};

export const TaskCpuStackTraceLink = ({
  taskId,
  attemptNumber,
  nodeId,
}: TaskProfilingStackTraceProps) => {
  const profilingEnabled = useProfilingEnabled();
  const defaults = useProfilingDefaults();
  if (!taskId) {
    return null;
  }
  if (!profilingEnabled) {
    return <DisabledProfilingLabel>Stack&nbsp;Trace</DisabledProfilingLabel>;
  }
  return (
    <ProfilingParamsDialog
      label={<React.Fragment>Stack&nbsp;Trace</React.Fragment>}
      dialogTitle="Stack Trace Config"
      triggerTitle="Sample the current stack trace for this worker."
      submitLabel="Get stack trace"
      flags={stackTraceFlags(defaults)}
      buildUrl={({ flags }) =>
        `task/traceback?task_id=${taskId}&attempt_number=${attemptNumber}` +
        `&node_id=${nodeId}${flagQuery(flags)}`
      }
    />
  );
};

export const CpuStackTraceLink = ({
  pid,
  nodeId,
  type = "",
}: CpuProfilingLinkProps) => {
  const profilingEnabled = useProfilingEnabled();
  const defaults = useProfilingDefaults();
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
    <ProfilingParamsDialog
      label={
        <React.Fragment>
          Stack&nbsp;Trace{type ? ` (${type})` : ""}
        </React.Fragment>
      }
      dialogTitle="Stack Trace Config"
      triggerTitle="Sample the current stack trace for this worker."
      submitLabel="Get stack trace"
      flags={stackTraceFlags(defaults)}
      buildUrl={({ flags }) =>
        `worker/traceback?pid=${pid}&node_id=${nodeId}${flagQuery(flags)}`
      }
    />
  );
};

export const CpuProfilingLink = ({
  pid,
  nodeId,
  type = "",
}: CpuProfilingLinkProps) => {
  const profilingEnabled = useProfilingEnabled();
  const defaults = useProfilingDefaults();
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
    <ProfilingParamsDialog
      label={
        <React.Fragment>
          CPU&nbsp;Flame&nbsp;Graph{type ? ` (${type})` : ""}
        </React.Fragment>
      }
      dialogTitle="CPU Profiling Config"
      triggerTitle="Profile the Python worker and display a CPU flame graph."
      submitLabel="Generate report"
      duration={{ initial: defaults.cpuDuration, max: defaults.maxDuration }}
      format={{ initial: defaults.cpuFormat, options: CPU_FORMAT_OPTIONS }}
      flags={cpuProfileFlags(defaults)}
      buildUrl={({ duration, format, flags }) =>
        `worker/cpu_profile?pid=${pid}&node_id=${nodeId}` +
        `&duration=${duration}&format=${format}${flagQuery(flags)}`
      }
    />
  );
};

const MEMORY_FORMAT_OPTIONS = [
  { value: "flamegraph", label: "Flamegraph" },
  { value: "table", label: "Table" },
];

export const ProfilerButton = ({
  profilerUrl,
  type,
  defaults = DEFAULT_PROFILING_DEFAULTS,
}: MemoryProfilingButtonProps) => {
  return (
    <ProfilingParamsDialog
      label={
        <React.Fragment>
          Memory&nbsp;Profiling{type ? ` (${type})` : ""}
        </React.Fragment>
      }
      dialogTitle="Memory Profiling Config"
      triggerTitle="Profile the memory usage of this worker."
      submitLabel="Generate report"
      duration={{ initial: defaults.memoryDuration, max: defaults.maxDuration }}
      format={{
        initial: defaults.memoryFormat,
        options: MEMORY_FORMAT_OPTIONS,
      }}
      // Flag order matches the historical query string:
      // leaks, native, trace_python_allocators.
      flags={[
        flag(
          "leaks",
          "Leaks",
          "Enable memory leaks, instead of peak memory usage. Refer to Memray " +
            "documentation for more details.",
          defaults.leaks,
        ),
        flag(
          "native",
          "Native",
          "Track native (C/C++) stack frames. Refer to Memray documentation " +
            "for more details.",
          defaults.native,
        ),
        flag(
          "trace_python_allocators",
          "Python Allocator Tracing",
          "Record allocations made by the pymalloc allocator. Refer to Memray " +
            "documentation for more details.",
          defaults.tracePythonAllocators,
        ),
      ]}
      buildUrl={({ duration, format, flags }) =>
        `${profilerUrl}&format=${format}&duration=${duration}${flagQuery(
          flags,
        )}`
      }
    />
  );
};

export const MemoryProfilingButton = ({
  pid,
  nodeId,
  type = "",
}: MemoryProfilingProps) => {
  const profilingEnabled = useProfilingEnabled();
  const defaults = useProfilingDefaults();
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

  return (
    <ProfilerButton profilerUrl={profilerUrl} type={type} defaults={defaults} />
  );
};

export const TaskMemoryProfilingButton = ({
  taskId,
  attemptNumber,
  nodeId,
}: TaskMemoryProfilingProps) => {
  const profilingEnabled = useProfilingEnabled();
  const defaults = useProfilingDefaults();
  if (!taskId) {
    return null;
  }
  if (!profilingEnabled) {
    return (
      <DisabledProfilingLabel>Memory&nbsp;Profiling</DisabledProfilingLabel>
    );
  }
  const profilerUrl = `memory_profile?task_id=${taskId}&attempt_number=${attemptNumber}&node_id=${nodeId}`;

  return <ProfilerButton profilerUrl={profilerUrl} defaults={defaults} />;
};
