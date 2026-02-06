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

const CMDResult = () => {
  const { cmd, ip, pid } = useParams() as {
    cmd: string;
    ip: string;
    pid: string;
  };
  const [result, setResult] = useState<string>();
  const [option, setOption] = useState("gcutil");
  const [numIterations, setNumIterations] = useState(4);
  const [traceLoading, setTraceLoading] = useState(false);
  const { themeMode } = useContext(GlobalContext);
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
    setResult(
      "Starting Torch trace... This may take a few minutes depending on the number of iterations.",
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

      let filename = "gputrace.json";
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

      setResult(
        `Torch trace downloaded.\n\n` +
          `The trace was captured for ${numIterations} training iterations.\n` +
          `The trace can be viewed in Chrome's chrome://tracing or Perfetto UI.`,
      );
    } catch (error) {
      setResult(
        `Failed to capture Torch trace. Error: ${(error as Error).message}\n\n` +
          `Please ensure:\n` +
          `1. KINETO_USE_DAEMON=1 and KINETO_DAEMON_INIT_DELAY_S=5 env vars are set for the worker.\n` +
          `2. The process is running a PyTorch training script.\n` +
          `3. The 'dynolog' package is installed on the node.`,
      );
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
            `This will profile ${numIterations} training iterations (calls to optimizer.step()).\n` +
            `The trace can be viewed in Chrome's chrome://tracing or Perfetto UI.`,
        );
        break;
      default:
        setResult(`Command ${cmd} is not supported.`);
        break;
    }
  }, [cmd, executeJstat, ip, pid, numIterations]);

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
                    setNumIterations(Math.max(1, parseInt(e.target.value) || 1))
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
