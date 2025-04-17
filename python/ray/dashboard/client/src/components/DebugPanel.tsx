import ArrowDownwardIcon from "@mui/icons-material/ArrowDownward";
import ArrowUpwardIcon from "@mui/icons-material/ArrowUpward";
import BugReportIcon from "@mui/icons-material/BugReport";
import BreakpointIcon from "@mui/icons-material/FiberManualRecord";
import PauseIcon from "@mui/icons-material/Pause";
import PlayArrowIcon from "@mui/icons-material/PlayArrow";
import SkipNextIcon from "@mui/icons-material/SkipNext";
import {
  Box,
  Button,
  Chip,
  CircularProgress,
  Collapse,
  IconButton,
  List,
  ListItem,
  ListItemButton,
  ListItemText,
  Paper,
  Stack,
  TextField,
  Typography,
} from "@mui/material";
import React, { useEffect, useRef, useState } from "react";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { atomDark } from "react-syntax-highlighter/dist/esm/styles/prism";
import { removeSlashes } from "slashes";
import {
  activateDebugSession,
  Breakpoint,
  deactivateDebugSession,
  DebugSession,
  getActiveDebugSessions,
  getBreakpoints,
  getDebugSessions,
  sendDebugCommand,
  storeBreakpoints,
} from "../service/debug";

type DebugPanelProps = {
  jobId?: string;
  selectedElement: any;
};

const SourceCodeView: React.FC<{
  sourceCode: string | null;
  currentLine?: number;
  isLoading: boolean;
  switchFrame: boolean;
  onLineClick?: (line: number) => void;
  selectedSourceFile?: any;
  selectedSession?: string;
  jobId?: string;
}> = ({
  sourceCode,
  currentLine,
  isLoading,
  switchFrame,
  onLineClick,
  selectedSourceFile,
  selectedSession,
  jobId,
}) => {
  const [breakpoints, setBreakpoints] = useState<number[]>([]);
  const codeContainerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (jobId && selectedSession) {
      getBreakpoints(jobId, selectedSession)
        .then((breakpoints) => {
          const currentBreakpoints = breakpoints
            .filter(
              (bp: Breakpoint) => bp.sourceFile === selectedSourceFile.path,
            )
            .map((bp: Breakpoint) => bp.line);
          setBreakpoints(currentBreakpoints);
        })
        .catch((error) => {
          console.error("Failed to get breakpoints:", error);
        });
    }
    // eslint-disable-next-line
  }, [selectedSourceFile?.path]);

  // Auto-scroll to current line when it changes
  useEffect(() => {
    if (currentLine && codeContainerRef.current && switchFrame) {
      const lineElement = codeContainerRef.current.querySelector(
        `[data-line-number="${currentLine}"]`,
      );
      if (lineElement) {
        // Get the current scroll position
        const currentScrollTop = codeContainerRef.current.scrollTop;
        const containerHeight = codeContainerRef.current.clientHeight;

        // Get line position relative to the container
        const lineRect = lineElement.getBoundingClientRect();
        const containerRect = codeContainerRef.current.getBoundingClientRect();
        const lineRelativeTop = lineRect.top - containerRect.top;

        const targetScrollTop =
          currentScrollTop +
          lineRelativeTop -
          containerHeight / 2 +
          lineRect.height / 2;

        codeContainerRef.current.scrollTo({
          top: targetScrollTop,
          behavior: "smooth",
        });
      }
    }
  }, [currentLine, sourceCode, switchFrame]);

  const handleLineClick = async (lineNumber: number) => {
    if (!selectedSourceFile || !selectedSession || !jobId) {
      return;
    }

    try {
      // Toggle breakpoint
      let newBreakpoints = [...breakpoints];

      if (newBreakpoints.includes(lineNumber)) {
        // Remove the breakpoint
        newBreakpoints = newBreakpoints.filter((line) => line !== lineNumber);
      } else {
        // Add the breakpoint
        newBreakpoints.push(lineNumber);
      }

      // Set all breakpoints at once
      const response = await sendDebugCommand(
        jobId,
        selectedSession,
        "set_breakpoints",
        {
          source: selectedSourceFile,
          lines: newBreakpoints,
        },
      );

      if (response && response.result) {
        // Update breakpoints from the response
        if (response.result.body && response.result.body.breakpoints) {
          const verifiedBreakpoints = response.result.body.breakpoints
            .filter((bp: any) => bp.verified)
            .map((bp: any) => bp.line);
          setBreakpoints(verifiedBreakpoints);
          const storedBreakpoints = verifiedBreakpoints.map((line: number) => ({
            sourceFile: selectedSourceFile.path,
            line: line,
          }));
          // eslint-disable-next-line
          storeBreakpoints(jobId!, selectedSession!, storedBreakpoints);
        }
      }
    } catch (error) {
      console.error("Failed to set breakpoints:", error);
    }
  };

  if (isLoading) {
    return (
      <Box
        sx={{
          display: "flex",
          justifyContent: "center",
          alignItems: "center",
          height: "100%",
        }}
      >
        <CircularProgress size={24} />
      </Box>
    );
  }

  if (!sourceCode) {
    return (
      <Box
        sx={{
          p: 2,
          display: "flex",
          justifyContent: "center",
          alignItems: "center",
          height: "100%",
        }}
      >
        <Typography
          variant="body2"
          sx={{ color: "text.secondary", fontStyle: "italic" }}
        >
          No source code available
        </Typography>
      </Box>
    );
  }

  const codeWithLineNumbers = {
    showLineNumbers: true,
    wrapLines: true,
    lineProps: (lineNumber: number) => {
      const style = {
        display: "block",
        width: "100%",
        cursor: "pointer",
        position: "relative" as const,
      };

      // Add data attribute for line number
      const props = {
        style,
        onClick: () => handleLineClick(lineNumber),
        "data-line-number": lineNumber,
      };

      // Highlight current execution line
      if (currentLine === lineNumber) {
        return {
          ...props,
          style: {
            ...style,
            backgroundColor: "rgba(255, 255, 0, 0.2)",
            display: "flex",
            alignItems: "center",
            "&::before": {
              content: '""',
              position: "absolute",
              left: "-20px",
              width: "0",
              height: "0",
              borderTop: "6px solid transparent",
              borderBottom: "6px solid transparent",
              borderLeft: "10px solid #ffd700",
            },
          },
        };
      }

      // Highlight breakpoint lines
      if (breakpoints.includes(lineNumber)) {
        return {
          ...props,
          style: {
            ...style,
            backgroundColor: "rgba(255, 23, 68, 0.08)",
            borderLeft: "3px solid #ff1744",
          },
        };
      }

      return props;
    },
    renderLineNumbers: (lineNumber: number) => (
      <Box
        component="span"
        sx={{
          cursor: "pointer",
          display: "flex",
          alignItems: "center",
          justifyContent: "flex-start",
          position: "relative",
          pl: "18px",
          "&:hover": {
            color: "primary.main",
            textDecoration: "underline",
          },
        }}
        onClick={(e) => {
          e.stopPropagation();
          handleLineClick(lineNumber);
        }}
      >
        {breakpoints.includes(lineNumber) && (
          <BreakpointIcon
            fontSize="small"
            style={{
              fontSize: "1rem",
              color: "#ff1744",
              position: "absolute",
              left: "0px",
              zIndex: 100000,
              animation: "none",
              filter: "drop-shadow(0px 0px 1px rgba(0,0,0,0.3))",
            }}
          />
        )}
        {lineNumber}
      </Box>
    ),
  };

  return (
    <Box sx={{ position: "relative", height: "100%" }}>
      <Box
        ref={codeContainerRef}
        sx={{
          height: "100%",
          overflow: "auto",
          borderRadius: 1,
          "& pre": {
            margin: 0,
            fontSize: "0.75rem",
            minHeight: "100%",
          },
        }}
      >
        <SyntaxHighlighter
          language="python"
          style={atomDark}
          {...codeWithLineNumbers}
          wrapLongLines={false}
          showLineNumbers={true}
          customStyle={{
            margin: 0,
            padding: "12px",
            whiteSpace: "pre",
            wordWrap: "normal",
            overflowX: "auto",
            minHeight: "100%",
          }}
        >
          {sourceCode}
        </SyntaxHighlighter>
      </Box>
    </Box>
  );
};

const DebugPanel: React.FC<DebugPanelProps> = ({ jobId, selectedElement }) => {
  const [expanded, setExpanded] = useState<boolean>(false);
  const [sessions, setSessions] = useState<DebugSession[]>([]);
  const [activeSessions, setActiveSessions] = useState<string[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [selectedSession, setSelectedSession] = useState<string | null>(null);
  const [threads, setThreads] = useState<any[]>([]);
  const [selectedThread, setSelectedThread] = useState<number | null>(null);
  const [stackFrames, setStackFrames] = useState<any[]>([]);
  const [selectedFrame, setSelectedFrame] = useState<number | null>(null);
  const [evaluateExpression, setEvaluateExpression] = useState<string>("");
  const [evaluateResult, setEvaluateResult] = useState<string | null>(null);
  const [loadingEvaluate, setLoadingEvaluate] = useState<boolean>(false);
  const [sourceCode, setSourceCode] = useState<string | null>(null);
  const [loadingSourceCode, setLoadingSourceCode] = useState<boolean>(false);
  const [selectedSourceFile, setSelectedSourceFile] = useState<any>(null);
  const [isPaused, setIsPaused] = useState<boolean>(false);
  const [canContinue, setCanContinue] = useState<boolean>(false);
  const [canPause, setCanPause] = useState<boolean>(false);
  const [canStep, setCanStep] = useState<boolean>(false);
  const [isInitializing, setIsInitializing] = useState<boolean>(false);
  const [pollInterval, setPollInterval] = useState<NodeJS.Timeout | null>(null);
  const [currentLine, setCurrentLine] = useState<number | null>(null);
  const [processCmdType, setProcessCmdType] = useState<string>("");
  const [switchFrame, setSwitchFrame] = useState<boolean>(false);

  // Toggle the debug panel
  const toggleExpanded = () => {
    setExpanded(!expanded);
  };

  useEffect(() => {
    if (stackFrames && selectedFrame) {
      const currentLine = stackFrames.find(
        (frame: any) => frame.id === selectedFrame,
      )?.line;
      setCurrentLine(currentLine);
    }
  }, [stackFrames, selectedFrame]);

  useEffect(() => {
    if (
      processCmdType !== "" &&
      selectedSession &&
      selectedThread &&
      selectedFrame &&
      selectedSourceFile
    ) {
      setLoading(true);
      checkDebugSessionPaused(selectedSession, processCmdType);
    }
    // eslint-disable-next-line
  }, [
    processCmdType,
    selectedSession,
    selectedThread,
    selectedFrame,
    selectedSourceFile,
  ]);

  // Fetch debug sessions when a node is selected
  useEffect(() => {
    if (expanded && selectedElement && jobId) {
      fetchDebugSessions();
    }
    // eslint-disable-next-line
  }, [selectedElement, jobId, expanded]);

  // Fetch active debug sessions
  useEffect(() => {
    if (expanded && jobId) {
      fetchActiveDebugSessions();
    }
    // eslint-disable-next-line
  }, [expanded, jobId]);

  const fetchDebugSessions = async () => {
    if (!jobId || !selectedElement || !selectedElement.name) {
      return;
    }

    try {
      let className = "";
      const funcName = selectedElement.name;

      // If it's a method, get the actor class name
      if (selectedElement.type === "method" && selectedElement.actorId) {
        className = selectedElement.actorName + ":" + selectedElement.actorId;
      }

      const sessions = await getDebugSessions(jobId, className, funcName);
      setSessions(sessions);
    } catch (error) {
      console.error("Failed to fetch debug sessions:", error);
    }
  };

  // Fetch active debug sessions
  const fetchActiveDebugSessions = async () => {
    if (!jobId) {
      return;
    }

    try {
      const activeSessions = await getActiveDebugSessions(jobId);
      setActiveSessions(activeSessions);
      const className = selectedElement?.actorId
        ? selectedElement?.actorName + ":" + selectedElement?.actorId
        : "";
      const funcName = selectedElement?.name;
      const currentActiveSessions = sessions.find(
        (session) =>
          session.taskId === selectedSession &&
          session.className === className &&
          session.funcName === funcName,
      );
      if (!currentActiveSessions) {
        setSelectedSession(null);
        setThreads([]);
        setStackFrames([]);
        setSelectedThread(null);
        setSelectedFrame(null);
      }
    } catch (error) {
      console.error("Failed to fetch active debug sessions:", error);
    }
  };

  const checkDebugSessionPaused = async (
    session: string,
    processCmdType: string,
  ) => {
    if (pollInterval) {
      clearInterval(pollInterval);
    }
    const check = async () => {
      try {
        // Check if we can continue (which means the program is paused)
        // eslint-disable-next-line
        const frames = await getCallFrame(session, selectedThread!);
        if (frames) {
          setStackFrames(frames);
        }
        const isReady = await isDebugCommandValid(
          selectedThread || undefined,
          frames[0].id || undefined,
          session,
        );
        if (isReady) {
          // Program is paused and ready for debugging
          const isStep =
            processCmdType === "step_over" ||
            processCmdType === "step_into" ||
            processCmdType === "step_out" ||
            processCmdType === "continue" ||
            processCmdType === "pause";
          clearInterval(interval);
          // Only fetch source code if it's a different file
          if (selectedThread) {
            if (
              !sourceCode ||
              !selectedSourceFile ||
              selectedSourceFile.path !== frames[0].source.path ||
              processCmdType === "switch"
            ) {
              await fetchSourceCode(
                session,
                selectedThread,
                isStep ? frames[0].id : selectedFrame,
                isStep ? frames[0].source : selectedSourceFile,
              );
            }
            if (isStep) {
              setSelectedFrame(frames[0].id);
              setSelectedSourceFile(frames[0].source);
            }
          }
          // Update debug state after confirming the program is paused
          setIsPaused(true);
          setCanContinue(true);
          setCanPause(false);
          setCanStep(true);
          setLoading(false);
          // Only trigger switchFrame if we're actually changing frames
          setSwitchFrame(true);
          setTimeout(() => {
            setSwitchFrame(false);
          }, 100);
          setProcessCmdType("");
        }
      } catch (error) {
        console.error("Error during debug session initialization:", error);
      }
    };
    check();
    const interval = setInterval(check, 2000);
    setPollInterval(interval);
    const timeoutId = setTimeout(() => {
      clearInterval(interval);
      setLoading(false);
    }, 300000);
    return () => {
      clearInterval(interval);
      clearTimeout(timeoutId);
    };
  };

  // Activate a debug session
  const handleActivate = async (session: DebugSession) => {
    if (!jobId) {
      return;
    }

    setIsInitializing(true);

    try {
      const result = await activateDebugSession(
        jobId,
        session.className,
        session.funcName,
        session.taskId,
      );

      if (result) {
        fetchActiveDebugSessions();
        setSelectedSession(session.taskId);

        const threadIds = await fetchThreads(session.taskId);
        // Try to pause the execution
        if (threadIds) {
          for (const threadId of threadIds) {
            await sendDebugCommand(jobId, session.taskId, "pause", {
              thread_id: threadId,
            });
          }
        }
        setIsInitializing(false);
        setTimeout(async () => {
          setSelectedSession(session.taskId);
          if (threadIds) {
            await fetchStackTrace(session.taskId, threadIds[0]);
          }
          setProcessCmdType("switch");
        }, 100);
      } else {
        setIsInitializing(false);
      }
    } catch (error) {
      console.error("Failed to activate debug session:", error);
      setIsInitializing(false);
    } finally {
    }
  };

  // Deactivate a debug session
  const handleDeactivate = async (taskId: string) => {
    if (!jobId) {
      return;
    }

    setLoading(true);
    try {
      const result = await deactivateDebugSession(jobId, taskId);

      if (result) {
        fetchActiveDebugSessions();
        if (selectedSession === taskId) {
          setSelectedSession(null);
          setThreads([]);
          setStackFrames([]);
          setSelectedThread(null);
          setSelectedFrame(null);
        }
      }
    } catch (error) {
      console.error("Failed to deactivate debug session:", error);
    } finally {
      setLoading(false);
    }
  };

  // Fetch threads for a debug session
  const fetchThreads = async (taskId: string): Promise<number[] | null> => {
    if (!jobId) {
      return null;
    }

    try {
      const response = await sendDebugCommand(jobId, taskId, "get_threads", {});
      if (response && response.result) {
        if (response.result.success === false) {
          await handleSessionUnavailable(taskId);
          return null;
        }

        if (response.result.body) {
          setThreads(response.result.body.threads || []);
          // Select the first thread by default if none is selected
          if (
            response.result.body.threads &&
            response.result.body.threads.length > 0
          ) {
            setSelectedThread(response.result.body.threads[0].id);
            return response.result.body.threads.map((thread: any) => thread.id);
          }
        }
      }
    } catch (error) {
      console.error("Failed to fetch threads:", error);
      // If we can't fetch threads, assume session is unavailable and remove it
      await handleSessionUnavailable(taskId);
      return null;
    }
    return null;
  };

  // Handle an unavailable session (could no longer be running)
  const handleSessionUnavailable = async (taskId: string) => {
    if (!jobId) {
      return;
    }

    try {
      // Remove the task from active sessions
      setActiveSessions((prev) => prev.filter((id) => id !== taskId));

      // Clear session data if this was the selected session
      if (selectedSession === taskId) {
        setSelectedSession(null);
        setThreads([]);
        setStackFrames([]);
        setSelectedThread(null);
        setSelectedFrame(null);
      }

      await fetchActiveDebugSessions();
    } catch (error) {
      console.error("Error handling unavailable session:", error);
    }
  };

  // Fetch source code for the current frame
  const fetchSourceCode = async (
    taskId: string,
    threadId: number,
    frameId: number,
    source: any,
  ): Promise<string | null> => {
    if (!jobId || !source) {
      return null;
    }

    setLoadingSourceCode(true);
    try {
      // 1. Open the file
      const openResponse = await sendDebugCommand(jobId, taskId, "evaluate", {
        expression: `_f_visual_debug_var_ = open("${source.path}")`,
        thread_id: threadId,
        frame_id: frameId,
      });

      if (!openResponse?.result?.success) {
        console.error(
          "Failed to open source file:",
          openResponse?.result?.message,
        );
        setLoadingSourceCode(false);
        return null;
      }

      // 2. Read the file content
      const readResponse = await sendDebugCommand(jobId, taskId, "evaluate", {
        expression: "_content_visual_debug_var_ = _f_visual_debug_var_.read()",
        thread_id: threadId,
        frame_id: frameId,
      });

      if (!readResponse?.result?.success) {
        // Try to close the file even if reading failed
        await sendDebugCommand(jobId, taskId, "evaluate", {
          expression: "_f_visual_debug_var_.close()",
          thread_id: threadId,
          frame_id: frameId,
        });

        setLoadingSourceCode(false);
        return null;
      }

      const lengthResponse = await sendDebugCommand(jobId, taskId, "evaluate", {
        expression: "len(_content_visual_debug_var_)",
        thread_id: threadId,
        frame_id: frameId,
      });

      if (!lengthResponse?.result?.body?.result) {
        await sendDebugCommand(jobId, taskId, "evaluate", {
          expression: "_f_visual_debug_var_.close()",
          thread_id: threadId,
          frame_id: frameId,
        });
        setLoadingSourceCode(false);
        return null;
      }

      const length = parseInt(lengthResponse.result.body.result);
      let content = "";
      let start_pos = 0;
      while (start_pos < length) {
        const sliceResponse = await sendDebugCommand(
          jobId,
          taskId,
          "evaluate",
          {
            expression:
              "_content_visual_debug_var_[" +
              start_pos.toString() +
              ":" +
              (start_pos + 20000).toString() +
              "]",
            thread_id: threadId,
            frame_id: frameId,
          },
        );
        content += sliceResponse.result.body.result.slice(1, -1);
        start_pos += 20000;
      }

      // 3. Close the file
      await sendDebugCommand(jobId, taskId, "evaluate", {
        expression: "_f_visual_debug_var_.close()",
        thread_id: threadId,
        frame_id: frameId,
      });

      // Set the source code and log it to console
      if (content) {
        // Process the source content by properly handling the string returned from Python
        const processedSource = removeSlashes(content);
        setSourceCode(processedSource);
        return processedSource;
      }
    } catch (error) {
      console.error("Failed to fetch source code:", error);
    } finally {
      setLoadingSourceCode(false);
    }
    return null;
  };

  // Fetch stack trace for a thread
  const getCallFrame = async (
    taskId: string,
    threadId: number,
  ): Promise<any> => {
    if (!jobId) {
      return null;
    }

    try {
      const response = await sendDebugCommand(
        jobId,
        taskId,
        "get_stack_trace",
        { thread_id: threadId },
      );

      if (response && response.result) {
        if (response.result.success === false) {
          return [null, null];
        }

        if (response.result.body) {
          return response.result.body.stackFrames;
        }
      }
    } catch (error) {
      console.error("Failed to fetch stack trace:", error);
      return null;
    }
    return null;
  };

  // Fetch stack trace for a thread
  const fetchStackTrace = async (
    taskId: string,
    threadId: number,
  ): Promise<[number | null, any | null]> => {
    if (!jobId) {
      return [null, null];
    }

    try {
      const response = await sendDebugCommand(
        jobId,
        taskId,
        "get_stack_trace",
        { thread_id: threadId },
      );

      if (response && response.result) {
        if (response.result.success === false) {
          return [null, null];
        }

        if (response.result.body) {
          setStackFrames(response.result.body.stackFrames || []);
          // Always select the first frame by default when frames are loaded
          if (
            response.result.body.stackFrames &&
            response.result.body.stackFrames.length > 0
          ) {
            const frameId = response.result.body.stackFrames[0].id;
            setSelectedFrame(frameId);

            // Set the selected source file for the first frame
            const firstFrame = response.result.body.stackFrames[0];
            if (firstFrame && firstFrame.source) {
              setSelectedSourceFile(firstFrame.source);
            }
            return [frameId, firstFrame.source];
          }
        }
      }
    } catch (error) {
      console.error("Failed to fetch stack trace:", error);
      return [null, null];
    }
    return [null, null];
  };

  // Handle thread selection
  const handleThreadSelect = async (threadId: number) => {
    if (!jobId || !selectedSession) {
      return;
    }

    const [frameId, source] = await fetchStackTrace(selectedSession, threadId);
    setSelectedFrame(frameId);
    setSelectedSourceFile(source);
    setProcessCmdType("switch");
    setSelectedThread(threadId);
  };

  // Handle frame selection
  const handleFrameSelect = async (frameId: number) => {
    if (!selectedSession || !selectedThread) {
      return;
    }

    setSelectedFrame(frameId);

    // Set the selected source file when a frame is selected
    const frame = stackFrames.find((f) => f.id === frameId);
    if (frame && frame.source) {
      // Only update source file and fetch source code if it's a different file
      if (
        !selectedSourceFile ||
        selectedSourceFile.path !== frame.source.path
      ) {
        setSelectedSourceFile(frame.source);
      }
    }
    setProcessCmdType("switch");
  };

  // Handle evaluating expressions
  const handleEvaluate = async () => {
    if (!jobId || !selectedSession || !selectedFrame || !selectedThread) {
      return;
    }

    setLoadingEvaluate(true);
    try {
      const response = await sendDebugCommand(
        jobId,
        selectedSession,
        "evaluate",
        {
          expression: evaluateExpression,
          frame_id: selectedFrame,
          thread_id: selectedThread,
        },
      );

      if (response && response.result) {
        if (response.result.success === false) {
          setEvaluateResult(
            `Error: ${response.result.message || "Evaluation failed"}`,
          );
          return;
        }

        if (response.result.body) {
          setEvaluateResult(response.result.body.result);
        }
      }
    } catch (error) {
      console.error("Failed to evaluate expression:", error);
      setEvaluateResult("Error evaluating expression");
    } finally {
      setLoadingEvaluate(false);
    }
  };

  // Handle debug control commands with feedback
  const executeDebugCommand = async (
    command: string,
    threadId?: number,
    message?: string,
  ) => {
    if (!jobId || !selectedSession) {
      return;
    }

    try {
      const args: Record<string, any> = {};
      if (threadId) {
        args.thread_id = threadId;
      }

      const response = await sendDebugCommand(
        jobId,
        selectedSession,
        command,
        args,
      );

      if (response && response.result) {
        if (response.result.success === false) {
          return false;
        }
        return true;
      }
    } catch (error) {
      console.error(`Failed to execute ${command}:`, error);
      return false;
    }
    return false;
  };

  // Check if a debug command is valid to execute
  const isDebugCommandValid = async (
    threadId?: number,
    frameId?: number,
    session?: string,
  ): Promise<boolean> => {
    if (!jobId) {
      return false;
    }

    const realThreadId = threadId || selectedThread;
    const realFrameId = frameId || selectedFrame;

    try {
      // Use an empty evaluate command to check if code is paused
      const response = await sendDebugCommand(
        jobId,
        session || selectedSession || "",
        "evaluate",
        {
          expression: "1",
          thread_id: realThreadId,
          frame_id: realFrameId,
        },
      );

      if (
        response &&
        response.result &&
        response.result.body &&
        response.result.body.result
      ) {
        const isPausedState = response.result.body.result === "1";

        return isPausedState;
      }
      return false;
    } catch (error) {
      return false;
    }
  };

  // Debug control commands with feedback
  const handleContinue = async () => {
    await executeDebugCommand(
      "continue",
      undefined,
      "Execution continued until next breakpoint or end of program",
    );
    setCanContinue(false);
    setCanPause(true);
    setCanStep(false);
    setIsPaused(false);
    setProcessCmdType("continue");
  };

  const handlePause = async () => {
    if (!selectedThread) {
      return;
    }
    setCanContinue(false);
    setCanPause(false);
    setCanStep(false);
    setIsPaused(false);

    await executeDebugCommand(
      "pause",
      selectedThread,
      "Execution paused - you can now inspect the current state",
    );
    setProcessCmdType("pause");
  };

  const handleStepOver = async () => {
    if (!selectedThread) {
      return;
    }
    setCanContinue(false);
    setCanPause(false);
    setCanStep(false);
    setIsPaused(false);

    await executeDebugCommand(
      "step_over",
      selectedThread,
      "Stepped over current line - execution proceeds to next line",
    );
    setProcessCmdType("step_over");
  };

  const handleStepInto = async () => {
    if (!selectedThread) {
      return;
    }
    setCanContinue(false);
    setCanPause(false);
    setCanStep(false);
    setIsPaused(false);

    await executeDebugCommand(
      "step_into",
      selectedThread,
      "Stepped into function call - now inside function body",
    );
    setProcessCmdType("step_into");
  };

  const handleStepOut = async () => {
    if (!selectedThread) {
      return;
    }
    setCanContinue(false);
    setCanPause(false);
    setCanStep(false);
    setIsPaused(false);

    await executeDebugCommand(
      "step_out",
      selectedThread,
      "Stepped out of current function - execution returns to caller",
    );
    setProcessCmdType("step_out");
  };

  const renderDebugControls = () => (
    <Stack direction="row" spacing={0.5} sx={{ mb: 1 }}>
      <span>
        <IconButton
          size="small"
          onClick={handleContinue}
          color="primary"
          disabled={!canContinue}
        >
          <PlayArrowIcon fontSize="small" />
        </IconButton>
      </span>
      <span>
        <IconButton
          size="small"
          onClick={handlePause}
          color="primary"
          disabled={!canPause}
        >
          <PauseIcon fontSize="small" />
        </IconButton>
      </span>
      <span>
        <IconButton
          size="small"
          onClick={handleStepOver}
          color="primary"
          disabled={!canStep}
        >
          <SkipNextIcon fontSize="small" />
        </IconButton>
      </span>
      <span>
        <IconButton
          size="small"
          onClick={handleStepInto}
          color="primary"
          disabled={!canStep}
        >
          <ArrowDownwardIcon fontSize="small" />
        </IconButton>
      </span>
      <span>
        <IconButton
          size="small"
          onClick={handleStepOut}
          color="primary"
          disabled={!canStep}
        >
          <ArrowUpwardIcon fontSize="small" />
        </IconButton>
      </span>
    </Stack>
  );

  return (
    <Box
      sx={{
        position: "absolute",
        top: 10,
        left: 200,
        zIndex: 99999,
      }}
    >
      <Button
        variant="contained"
        color="primary"
        size="small"
        startIcon={<BugReportIcon />}
        onClick={toggleExpanded}
        sx={{ mb: 1, borderRadius: 2 }}
      >
        {expanded ? "Hide" : "Debug"}
      </Button>

      <Collapse in={expanded}>
        <Box
          sx={{
            width: "95vw",
            maxWidth: 1200,
            p: 2,
            background: "transparent",
          }}
        >
          {isInitializing ? (
            <Paper
              elevation={2}
              sx={{
                p: 2,
                borderRadius: 2,
                display: "flex",
                flexDirection: "column",
                alignItems: "center",
                justifyContent: "center",
                height: 200,
              }}
            >
              <CircularProgress size={40} sx={{ mb: 2 }} />
              <Typography variant="body1" sx={{ textAlign: "center" }}>
                Initializing debug session...
              </Typography>
              <Typography
                variant="body2"
                sx={{ textAlign: "center", color: "text.secondary", mt: 1 }}
              >
                Waiting for program to pause at a breakpoint
              </Typography>
            </Paper>
          ) : (
            <Box
              sx={{
                display: "grid",
                gridTemplateColumns: "1fr 350px",
                gap: 2,
                background: "transparent",
              }}
            >
              {/* Source Code Card - Left side */}
              <Paper
                elevation={2}
                sx={{
                  p: 2,
                  borderRadius: 2,
                  height: "564px", // Matches right side: (180px * 3) + (gap * 2)
                  position: "relative",
                  background: "white",
                  display: "flex",
                  flexDirection: "column",
                  overflow: "hidden",
                }}
              >
                <Typography
                  variant="h6"
                  sx={{
                    mb: 2,
                    fontSize: "1rem",
                    fontWeight: "bold",
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "space-between",
                  }}
                >
                  <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                    <span>Source Code</span>
                    {loadingSourceCode && <CircularProgress size={16} />}
                  </Box>
                  <Box>{renderDebugControls()}</Box>
                </Typography>
                <Box sx={{ p: 1, flexGrow: 1, overflow: "auto" }}>
                  {selectedSourceFile && selectedSession && (
                    <Box sx={{ mb: 1 }}>
                      <Typography
                        variant="caption"
                        sx={{ display: "block", mb: 0.5 }}
                      >
                        File:{" "}
                        <Chip
                          size="small"
                          label={selectedSourceFile.path.split("/").pop()}
                        />
                        {currentLine && (
                          <Chip
                            size="small"
                            color="primary"
                            label={`Line: ${currentLine}`}
                            sx={{ ml: 0.5 }}
                          />
                        )}
                      </Typography>
                    </Box>
                  )}
                  {loadingSourceCode ? (
                    <Box
                      sx={{
                        display: "flex",
                        justifyContent: "center",
                        alignItems: "center",
                        height: "300px",
                      }}
                    >
                      <CircularProgress size={32} />
                    </Box>
                  ) : (
                    selectedSession && (
                      <SourceCodeView
                        sourceCode={sourceCode}
                        switchFrame={switchFrame}
                        currentLine={currentLine || undefined}
                        isLoading={loadingSourceCode}
                        selectedSourceFile={selectedSourceFile}
                        selectedSession={selectedSession || undefined}
                        jobId={jobId}
                      />
                    )
                  )}
                </Box>
              </Paper>

              {/* Right Side Cards Stack */}
              <Box
                sx={{
                  display: "flex",
                  flexDirection: "column",
                  gap: 2,
                  background: "transparent",
                  height: "564px", // Same as left side
                }}
              >
                {/* Sessions Card */}
                <Paper
                  elevation={2}
                  sx={{
                    p: 2,
                    borderRadius: 2,
                    height: "180px",
                    position: "relative",
                    background: "white",
                    display: "flex",
                    flexDirection: "column",
                    overflow: "hidden",
                  }}
                >
                  <Typography
                    variant="h6"
                    sx={{
                      mb: 2,
                      fontSize: "1rem",
                      fontWeight: "bold",
                      display: "flex",
                      alignItems: "center",
                      gap: 1,
                    }}
                  >
                    Debug Sessions
                  </Typography>
                  <Box sx={{ flexGrow: 1, overflow: "auto" }}>
                    <List dense disablePadding>
                      {sessions.length === 0 ? (
                        <ListItem>
                          <ListItemText primary="No debug sessions available" />
                        </ListItem>
                      ) : (
                        sessions.map((session) => (
                          <ListItem
                            key={session.taskId}
                            disablePadding
                            secondaryAction={
                              activeSessions.includes(session.taskId) ? (
                                <Button
                                  size="small"
                                  variant="outlined"
                                  color="error"
                                  onClick={() =>
                                    handleDeactivate(session.taskId)
                                  }
                                  disabled={
                                    isInitializing &&
                                    selectedSession === session.taskId
                                  }
                                  sx={{ py: 0, px: 1, minWidth: "auto" }}
                                >
                                  Stop
                                </Button>
                              ) : (
                                <Button
                                  size="small"
                                  variant="outlined"
                                  color="primary"
                                  onClick={() => handleActivate(session)}
                                  disabled={isInitializing}
                                  sx={{ py: 0, px: 1, minWidth: "auto" }}
                                >
                                  Start
                                </Button>
                              )
                            }
                          >
                            <ListItemButton
                              selected={selectedSession === session.taskId}
                              onClick={async () => {
                                if (
                                  activeSessions.includes(session.taskId) &&
                                  !isInitializing
                                ) {
                                  setSelectedSession(session.taskId);

                                  const threadIds = await fetchThreads(
                                    session.taskId,
                                  );
                                  if (threadIds) {
                                    await fetchStackTrace(
                                      session.taskId,
                                      threadIds[0],
                                    );
                                  }
                                  setProcessCmdType("switch");
                                }
                              }}
                              disabled={
                                !activeSessions.includes(session.taskId) ||
                                isInitializing
                              }
                              dense
                              sx={{ borderRadius: 1 }}
                            >
                              <ListItemText
                                primary={`${session.funcName}`}
                                secondary={`${session.taskId.substring(
                                  0,
                                  8,
                                )}...`}
                                primaryTypographyProps={{
                                  style: {
                                    fontWeight: activeSessions.includes(
                                      session.taskId,
                                    )
                                      ? "bold"
                                      : "normal",
                                    color: activeSessions.includes(
                                      session.taskId,
                                    )
                                      ? "#1976d2"
                                      : "inherit",
                                    fontSize: "0.875rem",
                                  },
                                }}
                                secondaryTypographyProps={{
                                  style: { fontSize: "0.75rem" },
                                }}
                              />
                            </ListItemButton>
                          </ListItem>
                        ))
                      )}
                    </List>
                  </Box>
                </Paper>

                {/* Debug Info Card */}
                <Paper
                  elevation={2}
                  sx={{
                    p: 2,
                    borderRadius: 2,
                    height: "180px",
                    position: "relative",
                    background: "white",
                    display: "flex",
                    flexDirection: "column",
                    overflow: "hidden",
                  }}
                >
                  <Typography
                    variant="h6"
                    sx={{
                      mb: 2,
                      fontSize: "1rem",
                      fontWeight: "bold",
                      display: "flex",
                      alignItems: "center",
                      gap: 1,
                    }}
                  >
                    Stack Info
                    {(loading || (selectedThread && !stackFrames.length)) && (
                      <CircularProgress size={16} />
                    )}
                  </Typography>
                  <Box sx={{ flexGrow: 1, overflow: "auto" }}>
                    {selectedSession && (
                      <React.Fragment>
                        <Typography
                          variant="caption"
                          sx={{ fontWeight: "bold", display: "block", mb: 0.5 }}
                        >
                          Threads
                        </Typography>
                        <List dense disablePadding sx={{ mb: 2 }}>
                          {threads.map((thread) => (
                            <ListItem
                              key={thread.id}
                              disablePadding
                              sx={{ mb: 0.5 }}
                            >
                              <ListItemButton
                                selected={selectedThread === thread.id}
                                onClick={() => handleThreadSelect(thread.id)}
                                dense
                                sx={{ borderRadius: 1, py: 0 }}
                              >
                                <ListItemText
                                  primary={thread.name}
                                  primaryTypographyProps={{
                                    style: { fontSize: "0.8rem" },
                                  }}
                                />
                              </ListItemButton>
                            </ListItem>
                          ))}
                        </List>

                        {selectedThread && selectedSession && (
                          <React.Fragment>
                            <Typography
                              variant="caption"
                              sx={{
                                fontWeight: "bold",
                                display: "block",
                                mb: 0.5,
                              }}
                            >
                              Stack Frames
                            </Typography>
                            <List dense disablePadding>
                              {stackFrames.map((frame) => (
                                <ListItem
                                  key={frame.id}
                                  disablePadding
                                  sx={{ m: 0 }}
                                >
                                  <ListItemButton
                                    selected={selectedFrame === frame.id}
                                    onClick={() => handleFrameSelect(frame.id)}
                                    dense
                                    sx={{
                                      borderRadius: 1,
                                      py: 0,
                                      minHeight: "24px",
                                    }}
                                  >
                                    <ListItemText
                                      primary={
                                        frame.name.split(":")[0] +
                                        " - " +
                                        frame.source.path.split("/").pop() +
                                        ":" +
                                        frame.line
                                      }
                                      primaryTypographyProps={{
                                        style: { fontSize: "0.75rem" },
                                      }}
                                      secondaryTypographyProps={{
                                        style: { fontSize: "0.7rem" },
                                      }}
                                    />
                                  </ListItemButton>
                                </ListItem>
                              ))}
                            </List>
                          </React.Fragment>
                        )}
                      </React.Fragment>
                    )}
                  </Box>
                </Paper>

                {/* Evaluate Card */}
                <Paper
                  elevation={2}
                  sx={{
                    p: 2,
                    borderRadius: 2,
                    height: "180px",
                    position: "relative",
                    background: "white",
                    display: "flex",
                    flexDirection: "column",
                    overflow: "hidden",
                  }}
                >
                  <Typography
                    variant="h6"
                    sx={{
                      mb: 2,
                      fontSize: "1rem",
                      fontWeight: "bold",
                      display: "flex",
                      alignItems: "center",
                      gap: 1,
                    }}
                  >
                    Evaluate Expression
                    {loadingEvaluate && <CircularProgress size={16} />}
                  </Typography>
                  <Box sx={{ flexGrow: 1, overflow: "auto" }}>
                    {loadingEvaluate ? (
                      <Box
                        sx={{
                          display: "flex",
                          justifyContent: "center",
                          alignItems: "center",
                          height: "100%",
                        }}
                      >
                        <CircularProgress size={24} />
                      </Box>
                    ) : selectedFrame && selectedSession && isPaused ? (
                      <React.Fragment>
                        <Box sx={{ display: "flex", mb: 1 }}>
                          <TextField
                            size="small"
                            fullWidth
                            variant="outlined"
                            value={evaluateExpression}
                            onChange={(e) =>
                              setEvaluateExpression(e.target.value)
                            }
                            placeholder="Enter expression..."
                            sx={{
                              mr: 1,
                              "& .MuiOutlinedInput-root": {
                                fontSize: "0.8rem",
                                "& fieldset": { borderRadius: 1.5 },
                              },
                            }}
                          />
                          <Button
                            variant="contained"
                            size="small"
                            onClick={handleEvaluate}
                            disabled={loadingEvaluate}
                            sx={{ py: 0.5 }}
                          >
                            {loadingEvaluate ? (
                              <CircularProgress size={16} />
                            ) : (
                              "Run"
                            )}
                          </Button>
                        </Box>
                        {evaluateResult !== null && (
                          <Paper
                            variant="outlined"
                            sx={{
                              p: 1,
                              mt: 1,
                              bgcolor: "grey.50",
                              borderRadius: 1.5,
                            }}
                          >
                            <Typography
                              variant="body2"
                              fontFamily="monospace"
                              sx={{ fontSize: "0.75rem" }}
                            >
                              {evaluateResult}
                            </Typography>
                          </Paper>
                        )}
                      </React.Fragment>
                    ) : (
                      <Box
                        sx={{
                          p: 2,
                          display: "flex",
                          justifyContent: "center",
                          alignItems: "center",
                          height: 150,
                        }}
                      >
                        <Typography
                          variant="body2"
                          sx={{ color: "text.secondary", fontStyle: "italic" }}
                        >
                          {!isPaused
                            ? "Code execution is running. Pause it to evaluate expressions."
                            : "Please select one stack frame first"}
                        </Typography>
                      </Box>
                    )}
                  </Box>
                </Paper>
              </Box>
            </Box>
          )}
        </Box>
      </Collapse>
    </Box>
  );
};

export default DebugPanel;
