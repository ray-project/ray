import {
  Box,
  Button,
  CircularProgress,
  FormControl,
  FormControlLabel,
  Grid,
  MenuItem,
  Radio,
  RadioGroup,
  Select,
  TextField,
  Typography,
} from "@mui/material";
import React, { useCallback, useContext, useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import { GlobalContext } from "../../App";
import LogVirtualView from "../../components/LogView/LogVirtualView";
import TitleCard from "../../components/TitleCard";
import {
  axiosInstance,
  formatUrl,
} from "../../service/requestHandlers";
import { getJmap, getJstack, getJstat } from "../../service/util";

const PERFETTO_UI_URL = "https://ui.perfetto.dev/";

const MIN_ITERATIONS = 1;
const MAX_ITERATIONS = 100;
const MIN_DURATION_SECONDS = 1;
const MAX_DURATION_SECONDS = 300;

const CMDResult = () => {
  const { cmd, ip, pid } = useParams() as {
    cmd: string;
    ip: string;
    pid: string;
  };
  const [result, setResult] = useState<string>();
  const [option, setOption] = useState("gcutil");
  const { themeMode } = useContext(GlobalContext);
  const [numIterations, setNumIterations] = useState(4);
  const [profilingMode, setProfilingMode] = useState<"iterations" | "duration">(
    "iterations",
  );
  const [durationSeconds, setDurationSeconds] = useState(30);
  const [traceLoading, setTraceLoading] = useState(false);
  const [traceSuccess, setTraceSuccess] = useState(false);
  const executeJstat = useCallback(
    () =>
      getJstat(ip, pid, option)
        .then((rsp) => {
          if (rsp.data.result) {
            setResult(rsp.data.data.output);
          } else {
            setResult(rsp.data.msg);
          }
        })
        .catch((err) => setResult(err.toString())),
    [ip, pid, option],
  );

  const executeTorchTrace = useCallback(async () => {
    setTraceLoading(true);
    setTraceSuccess(false);
    const modeDescription =
      profilingMode === "iterations"
        ? `${numIterations} training iterations`
        : `${durationSeconds} seconds`;
    setResult(
      `Starting Torch trace for ${modeDescription}...\n` +
        "This may take a few minutes. Server timeout is 5 minutes.",
    );
    try {
      const params: Record<string, string> = { ip, pid };
      if (profilingMode === "iterations") {
        params.num_iterations = String(numIterations);
      } else {
        params.duration_ms = String(durationSeconds * 1000);
      }

      const response = await axiosInstance.get(
        formatUrl("worker/gpu_profile"),
        {
          params,
          responseType: "blob",
        },
      );

      const blob = new Blob([response.data]);
      const downloadUrl = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.style.display = "none";
      a.href = downloadUrl;

      const now = new Date();
      const dateStr = now.toISOString().replace(/[:.]/g, "-").slice(0, 19);
      let filename = `gputrace_${dateStr}.json`;
      const contentDisposition = response.headers["content-disposition"];
      if (contentDisposition) {
        const filenameMatch = contentDisposition.match(/filename="(.+)"/);
        if (filenameMatch && filenameMatch.length === 2) {
          filename = filenameMatch[1];
        }
      }
      a.download = filename;

      document.body.appendChild(a);
      a.click();

      window.URL.revokeObjectURL(downloadUrl);
      a.remove();

      const timestamp = new Date().toLocaleString();
      const captureInfo =
        profilingMode === "iterations"
          ? `${numIterations} training iterations`
          : `${durationSeconds} seconds`;
      setResult(
        `Torch trace downloaded successfully!\n\n` +
          `Captured at: ${timestamp}\n` +
          `The trace was captured for ${captureInfo}.\n` +
          `Drag and drop the downloaded file into Perfetto UI to view it.`,
      );
      setTraceSuccess(true);
    } catch (error) {
      setResult(
        `Failed to capture Torch trace. Error: ${(error as Error).message}\n\n` +
          `Please ensure:\n` +
          `1. KINETO_USE_DAEMON=1 and KINETO_DAEMON_INIT_DELAY_S=5 env vars are set for the worker.\n` +
          `2. The process is running a PyTorch training script.\n` +
          `3. The 'dynolog' package is installed on the node.`,
      );
      setTraceSuccess(false);
    } finally {
      setTraceLoading(false);
    }
  }, [ip, pid, numIterations, profilingMode, durationSeconds]);

  useEffect(() => {
    switch (cmd) {
      case "jstack":
        getJstack(ip, pid)
          .then((rsp) => {
            if (rsp.data.result) {
              setResult(rsp.data.data.output);
            } else {
              setResult(rsp.data.msg);
            }
          })
          .catch((err) => setResult(err.toString()));
        break;
      case "jmap":
        getJmap(ip, pid)
          .then((rsp) => {
            if (rsp.data.result) {
              setResult(rsp.data.data.output);
            } else {
              setResult(rsp.data.msg);
            }
          })
          .catch((err) => setResult(err.toString()));
        break;
      case "jstat":
        executeJstat();
        break;
      case "torchtrace":
        setResult(
          `Click "Start Trace" to capture a Torch GPU profiling trace.\n\n` +
            `Choose iterations or duration mode, configure the value, then click Start Trace.`,
        );
        break;
      default:
        setResult(`Command ${cmd} is not supported.`);
        break;
    }
  }, [cmd, executeJstat, ip, pid]);

  return (
    <Box sx={{ padding: 4, width: "100%" }}>
      <TitleCard title={cmd}>
        {cmd === "jstat" && (
          <Box sx={{ padding: 2, marginTop: 2 }}>
            <Grid container spacing={1}>
              <Grid item>
                <Select
                  value={option}
                  onChange={(e) => setOption(e.target.value as string)}
                  sx={{ "& .MuiSvgIcon-root": { color: "text.secondary" } }}
                >
                  {[
                    "class",
                    "compiler",
                    "gc",
                    "gccapacity",
                    "gcmetacapacity",
                    "gcnew",
                    "gcnewcapacity",
                    "gcold",
                    "gcoldcapacity",
                    "gcutil",
                    "gccause",
                    "printcompilation",
                  ].map((e) => (
                    <MenuItem value={e}>{e}</MenuItem>
                  ))}
                </Select>
              </Grid>
              <Grid item>
                <Button onClick={executeJstat}>Execute</Button>
              </Grid>
            </Grid>
          </Box>
        )}
        {cmd === "torchtrace" && (
          <Box sx={{ padding: 2, marginTop: 2 }}>
            <Typography variant="body2" sx={{ marginBottom: 2 }}>
              Capture a PyTorch/Kineto GPU profiling trace using Dynolog.
            </Typography>
            <FormControl sx={{ marginBottom: 2 }}>
              <RadioGroup
                row
                value={profilingMode}
                onChange={(e) =>
                  setProfilingMode(e.target.value as "iterations" | "duration")
                }
              >
                <FormControlLabel
                  value="iterations"
                  control={<Radio size="small" />}
                  label="Iterations"
                />
                <FormControlLabel
                  value="duration"
                  control={<Radio size="small" />}
                  label="Duration"
                />
              </RadioGroup>
            </FormControl>
            <Grid container spacing={2} alignItems="center">
              <Grid item>
                {profilingMode === "iterations" ? (
                  <TextField
                    label="Iterations"
                    type="number"
                    size="small"
                    value={numIterations}
                    onChange={(e) =>
                      setNumIterations(
                        Math.min(
                          MAX_ITERATIONS,
                          Math.max(
                            MIN_ITERATIONS,
                            parseInt(e.target.value) || MIN_ITERATIONS,
                          ),
                        ),
                      )
                    }
                    inputProps={{ min: MIN_ITERATIONS, max: MAX_ITERATIONS }}
                    helperText="Number of optimizer.step() calls to profile"
                  />
                ) : (
                  <TextField
                    label="Duration (seconds)"
                    type="number"
                    size="small"
                    value={durationSeconds}
                    onChange={(e) =>
                      setDurationSeconds(
                        Math.min(
                          MAX_DURATION_SECONDS,
                          Math.max(
                            MIN_DURATION_SECONDS,
                            parseInt(e.target.value) || MIN_DURATION_SECONDS,
                          ),
                        ),
                      )
                    }
                    inputProps={{
                      min: MIN_DURATION_SECONDS,
                      max: MAX_DURATION_SECONDS,
                    }}
                    helperText="Time in seconds to profile (for data loaders)"
                  />
                )}
              </Grid>
              <Grid item>
                <Button
                  variant="contained"
                  onClick={executeTorchTrace}
                  disabled={traceLoading}
                  startIcon={traceLoading ? <CircularProgress size={20} /> : null}
                >
                  {traceLoading ? "Starting..." : "Start Trace"}
                </Button>
              </Grid>
            </Grid>
          </Box>
        )}
      </TitleCard>
      <TitleCard title={`IP: ${ip} / Pid: ${pid}`}>
        {traceSuccess && cmd === "torchtrace" && (
          <Box sx={{ mb: 2 }}>
            <Button
              variant="contained"
              color="primary"
              href={PERFETTO_UI_URL}
              target="_blank"
              rel="noopener noreferrer"
            >
              Open Perfetto UI
            </Button>
            <Typography variant="body2" sx={{ mt: 1, color: "text.secondary" }}>
              Drag and drop the downloaded trace file into Perfetto to view it.
            </Typography>
          </Box>
        )}
        <LogVirtualView
          content={result || "loading"}
          language="prolog"
          height={800}
          theme={themeMode}
        />
      </TitleCard>
    </Box>
  );
};

export default CMDResult;
