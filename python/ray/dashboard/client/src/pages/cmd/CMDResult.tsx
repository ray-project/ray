import {
  Box,
  Button,
  CircularProgress,
  Grid,
  MenuItem,
  Select,
  TextField,
  Typography,
} from "@mui/material";
import React, { useCallback, useContext, useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import { GlobalContext } from "../../App";
import LogVirtualView from "../../components/LogView/LogVirtualView";
import TitleCard from "../../components/TitleCard";
import { getJmap, getJstack, getJstat } from "../../service/util";

const PERFETTO_UI_URL = "https://ui.perfetto.dev/";

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
    setResult(
      "Starting Torch trace... This may take a few minutes depending on the number of iterations.\n" +
        "Server timeout is 5 minutes.",
    );
    try {
      const url = `/worker/gpu_profile?ip=${encodeURIComponent(
        ip,
      )}&pid=${encodeURIComponent(pid)}&num_iterations=${numIterations}`;
      const response = await fetch(url);

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(
          `Request failed: ${response.status} ${response.statusText}. ${errorText}`,
        );
      }

      const blob = await response.blob();
      const downloadUrl = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.style.display = "none";
      a.href = downloadUrl;

      const now = new Date();
      const dateStr = now.toISOString().replace(/[:.]/g, "-").slice(0, 19);
      let filename = `gputrace_${dateStr}.json`;
      const contentDisposition = response.headers.get("content-disposition");
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
      setResult(
        `Torch trace downloaded successfully!\n\n` +
          `Captured at: ${timestamp}\n` +
          `The trace was captured for ${numIterations} training iterations.\n` +
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
  }, [ip, pid, numIterations]);

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
            `Configure the number of training iterations (calls to optimizer.step()) to profile, then click Start Trace.`,
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
            <Grid container spacing={2} alignItems="center">
              <Grid item>
                <TextField
                  label="Iterations"
                  type="number"
                  size="small"
                  value={numIterations}
                  onChange={(e) =>
                    setNumIterations(
                      Math.min(100, Math.max(1, parseInt(e.target.value) || 1)),
                    )
                  }
                  inputProps={{ min: 1, max: 100 }}
                  helperText="Number of optimizer.step() calls to profile"
                />
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
