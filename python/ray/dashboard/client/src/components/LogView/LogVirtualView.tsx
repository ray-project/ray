import React, { useMemo, useState, useCallback } from "react";
import { Box, Typography } from "@mui/material";
import dayjs from "dayjs";
import DialogWithTitle from "../../common/DialogWithTitle";
import "./darcula.css";
import "./github.css";
import "./index.css";
import { MAX_LINES_FOR_LOGS } from "../../service/log";

const uniqueKeySelector = () => Math.random().toString(16).slice(-8);

const timeReg =
  /(?:(?!0000)[0-9]{4}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1[0-9]|2[0-8])|(?:0[13-9]|1[0-2])-(?:29|30)|(?:0[13578]|1[02])-31)|(?:[0-9]{2}(?:0[48]|[2468][048]|[13579][26])|(?:0[48]|[2468][048]|[13579][26])00)-02-29)\s+([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]/;

const value2react = ({ type, tagName, properties, children, value = "" }, key, keywords = "") => {
  switch (type) {
    case "element":
      return React.createElement(
        tagName,
        {
          className: properties.className[0],
          key: `${key}line${uniqueKeySelector()}`,
        },
        children.map((e, i) => value2react(e, `${key}-${i}`, keywords)),
      );
    case "text":
      if (keywords && value.includes(keywords)) {
        const afterChildren = [];
        const vals = value.split(keywords);
        let tmp = vals.shift();
        if (!tmp) {
          return React.createElement("span", { className: "find-kws" }, keywords);
        }
        while (typeof tmp === "string") {
          if (tmp !== "") {
            afterChildren.push(tmp);
          } else {
            afterChildren.push(React.createElement("span", { className: "find-kws" }, keywords));
          }

          tmp = vals.shift();
          if (tmp) {
            afterChildren.push(React.createElement("span", { className: "find-kws" }, keywords));
          }
        }
        return afterChildren;
      }
      return value;
    default:
      return [];
  }
};

const LogLineDetailDialog = ({ formattedLogLine, message, onClose }) => {
  return (
    <DialogWithTitle title="Log line details" handleClose={onClose}>
      <Box sx={{ display: "flex", flexDirection: "row", gap: 4, alignItems: "stretch" }}>
        <Box sx={{ width: "100%" }}>
          <Typography variant="h5" sx={{ marginBottom: 2 }}>
            Raw log line
          </Typography>
          <Box
            sx={(theme) => ({
              padding: 1,
              bgcolor: "#EEEEEE",
              borderRadius: 1,
              border: `1px solid ${theme.palette.divider}`,
            })}
          >
            <Typography component="pre" variant="body2" sx={{ whiteSpace: "pre", overflow: "auto", height: "300px" }} data-testid="raw-log-line">
              {formattedLogLine}
            </Typography>
          </Box>
          <Typography variant="h5" sx={{ marginTop: 2, marginBottom: 2 }}>
            Formatted message
          </Typography>
          <Box
            sx={(theme) => ({
              padding: 1,
              bgcolor: "#EEEEEE",
              borderRadius: 1,
              border: `1px solid ${theme.palette.divider}`,
            })}
          >
            <Typography component="pre" variant="body2" sx={{ whiteSpace: "pre", overflow: "auto", height: "300px" }} data-testid="raw-log-line">
              {message}
            </Typography>
          </Box>
        </Box>
      </Box>
    </DialogWithTitle>
  );
};

const LogVirtualView = ({
  content,
  width = "100%",
  height = 600,
  fontSize = 12,
  theme = "light",
  keywords = "",
  language = "dos",
  focusLine = 1,
  style = {},
  onScrollBottom,
  revert = false,
  startTime,
  endTime,
}) => {
  const [selectedLogLine, setSelectedLogLine] = useState(null);

  const logs = useMemo(() => {
    const originContent = content.split("\n");
    return originContent
      .map((e, i) => ({
        i,
        origin: e,
        time: (e?.match(timeReg) || [""])[0],
      }))
      .filter((e) => {
        let bool = e.origin.includes(keywords);
        if (e.time && startTime && !dayjs(e.time).isAfter(dayjs(startTime))) {
          bool = false;
        }
        if (e.time && endTime && !dayjs(e.time).isBefore(dayjs(endTime))) {
          bool = false;
        }
        return bool;
      });
  }, [content, keywords, startTime, endTime]);

  const handleLogLineClick = useCallback((logLine, message) => {
    setSelectedLogLine({ logLine, message });
  }, []);

  const renderedLogs = useMemo(
    () =>
      logs.map(({ i, origin }) => {
        let message = origin;
        let formattedLogLine = origin;
        try {
          const parsedOrigin = JSON.parse(origin);
          if (parsedOrigin.message) {
            message = parsedOrigin.message;
            if (parsedOrigin.levelname) {
              message = `${parsedOrigin.levelname} ${message}`;
            }
            if (parsedOrigin.asctime) {
              message = `${parsedOrigin.asctime}\t${message}`;
            }
          }
          formattedLogLine = JSON.stringify(parsedOrigin, null, 2);
        } catch (e) {}

        return (
          <Box
            key={`${i}list`}
            sx={{
              overflowX: "visible",
              whiteSpace: "nowrap",
              "&::before": {
                content: `"${i + 1}"`,
                marginRight: 0.5,
                width: `${logs.length}`.length * 6 + 4,
                color: "#999",
                display: "inline-block",
              },
            }}
            onClick={() => {
              if ((window.getSelection()?.toString().length ?? 0) === 0) {
                handleLogLineClick(formattedLogLine, message);
              }
            }}
          >
            {message.split(keywords).map((part, index) =>
              index === 0 ? (
                part
              ) : (
                <span key={index} className="find-kws">
                  {keywords}
                </span>
              )
            )}
            <br />
          </Box>
        );
      }),
    [logs, keywords, handleLogLineClick]
  );

  useEffect(() => {
    if (onScrollBottom && logs.length > 0) {
      const handleScroll = (event) => {
        const { target } = event;
        if (target.scrollTop + target.clientHeight === target.scrollHeight) {
          onScrollBottom(event);
        }
      };
      window.addEventListener("scroll", handleScroll);
      return () => window.removeEventListener("scroll", handleScroll);
    }
  }, [logs, onScrollBottom]);

  return (
    <div style={{ width, height, fontSize, fontFamily: "menlo, monospace", ...style }} className={`hljs-${theme}`}>
      {logs.length > MAX_LINES_FOR_LOGS && (
        <Box component="p" sx={{ color: (theme) => theme.palette.error.main }}>
          [Truncation warning] This log has been truncated and only the latest {MAX_LINES_FOR_LOGS} lines are displayed. Click "Download" button above to see the full log
        </Box>
      )}
      {renderedLogs}
      {selectedLogLine && (
        <LogLineDetailDialog
          formattedLogLine={selectedLogLine.logLine}
          message={selectedLogLine.message}
          onClose={() => setSelectedLogLine(null)}
        />
      )}
    </div>
  );
};

export default LogVirtualView;
