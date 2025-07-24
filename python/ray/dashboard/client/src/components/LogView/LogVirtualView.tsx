import { Box, Typography } from "@mui/material";
import dayjs from "dayjs";
import prolog from "highlight.js/lib/languages/prolog";
import { lowlight } from "lowlight";
import React, {
  MutableRefObject,
  useCallback,
  useEffect,
  useRef,
  useState,
} from "react";
import { FixedSizeList as List } from "react-window";
import DialogWithTitle from "../../common/DialogWithTitle";
import "./darcula.css";
import "./github.css";
import "./index.css";
import { MAX_LINES_FOR_LOGS } from "../../service/log";

lowlight.registerLanguage("prolog", prolog);

const uniqueKeySelector = () => Math.random().toString(16).slice(-8);

const timeReg =
  /(?:(?!0000)[0-9]{4}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1[0-9]|2[0-8])|(?:0[13-9]|1[0-2])-(?:29|30)|(?:0[13578]|1[02])-31)|(?:[0-9]{2}(?:0[48]|[2468][048]|[13579][26])|(?:0[48]|[2468][048]|[13579][26])00)-02-29)\s+([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]/;

const value2react = (
  { type, tagName, properties, children, value = "" }: any,
  key: string,
  keywords = "",
) => {
  switch (type) {
    case "element":
      return React.createElement(
        tagName,
        {
          className: properties.className[0],
          key: `${key}line${uniqueKeySelector()}`,
        },
        children.map((e: any, i: number) =>
          value2react(e, `${key}-${i}`, keywords),
        ),
      );
    case "text":
      if (keywords && value.includes(keywords)) {
        const afterChildren = [];
        const vals = value.split(keywords);
        let tmp = vals.shift();
        if (!tmp) {
          return React.createElement(
            "span",
            { className: "find-kws" },
            keywords,
          );
        }
        while (typeof tmp === "string") {
          if (tmp !== "") {
            afterChildren.push(tmp);
          } else {
            afterChildren.push(
              React.createElement("span", { className: "find-kws" }, keywords),
            );
          }

          tmp = vals.shift();
          if (tmp) {
            afterChildren.push(
              React.createElement("span", { className: "find-kws" }, keywords),
            );
          }
        }
        return afterChildren;
      }
      return value;
    default:
      return [];
  }
};

export type LogVirtualViewProps = {
  content: string;
  width?: number;
  height?: number;
  fontSize?: number;
  theme?: "light" | "dark";
  language?: string;
  focusLine?: number;
  keywords?: string;
  style?: { [key: string]: string | number };
  listRef?: MutableRefObject<HTMLDivElement | null>;
  onScrollBottom?: (event: Event) => void;
  revert?: boolean;
  startTime?: string;
  endTime?: string;
};

type LogLineDetailDialogProps = {
  formattedLogLine: string | null;
  message: string;
  onClose: () => void;
};

const LogLineDetailDialog = ({
  formattedLogLine,
  message,
  onClose,
}: LogLineDetailDialogProps) => {
  return (
    <DialogWithTitle title="Log line details" handleClose={onClose}>
      <Box
        sx={{
          display: "flex",
          flexDirection: "row",
          gap: 4,
          alignItems: "stretch",
        }}
      >
        <Box
          sx={{
            width: "100%",
          }}
        >
          {formattedLogLine !== null && (
            <React.Fragment>
              <Typography
                variant="h5"
                sx={{
                  marginBottom: 2,
                }}
              >
                Raw log line
              </Typography>
              <Box
                sx={(theme) => ({
                  padding: 1,
                  bgcolor: "#EEEEEE",
                  borderRadius: 1,
                  border: `1px solid ${theme.palette.divider}`,
                  marginBottom: 2,
                })}
              >
                <Typography
                  component="pre"
                  variant="body2"
                  sx={{
                    whiteSpace: "pre",
                    overflow: "auto",
                    height: "300px",
                  }}
                  data-testid="raw-log-line"
                >
                  {formattedLogLine}
                </Typography>
              </Box>
            </React.Fragment>
          )}
          <Typography
            variant="h5"
            sx={{
              marginBottom: 2,
            }}
          >
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
            <Typography
              component="pre"
              variant="body2"
              sx={{
                whiteSpace: "pre",
                overflow: "auto",
                height: "300px",
              }}
              data-testid="raw-log-line"
            >
              {message}
            </Typography>
          </Box>
        </Box>
      </Box>
    </DialogWithTitle>
  );
};

const LogVirtualView: React.FC<LogVirtualViewProps> = ({
  content,
  width = "100%",
  height,
  fontSize = 12,
  theme = "light",
  keywords = "",
  language = "dos",
  focusLine = 1,
  style = {},
  listRef,
  onScrollBottom,
  revert = false,
  startTime,
  endTime,
}) => {
  const [logs, setLogs] = useState<{ i: number; origin: string }[]>([]);
  const total = logs.length;
  const timmer = useRef<ReturnType<typeof setTimeout>>();
  const el = useRef<List>(null);
  const outter = useRef<HTMLDivElement>(null);
  if (listRef) {
    listRef.current = outter.current;
  }
  const [selectedLogLine, setSelectedLogLine] =
    useState<[string | null, string]>();
  const handleLogLineClick = useCallback(
    (logLine: string | null, message: string) => {
      setSelectedLogLine([logLine, message]);
    },
    [],
  );

  const itemRenderer = ({ index, style }: { index: number; style: any }) => {
    const { i, origin } = logs[revert ? logs.length - 1 - index : index];

    let message = origin;
    let formattedLogLine: string | null = null;
    try {
      const parsedOrigin = JSON.parse(origin);
      // Iff the parsed origin has a message field, use it as the message.
      if (parsedOrigin.message) {
        message = parsedOrigin.message;
        // If levelname exist on the structured logs, put it in front of the message.
        if (parsedOrigin.levelname) {
          message = `${parsedOrigin.levelname} ${message}`;
        }
        // If asctime exist on the structured logs, use it as the prefix of the message.
        if (parsedOrigin.asctime) {
          message = `${parsedOrigin.asctime}\t${message}`;
        }
      }
      formattedLogLine = JSON.stringify(parsedOrigin, null, 2);
    } catch (e) {
      // Keep the `origin` as message, if json parsing failed.
      // If formattedLogLine is null, then the log line is not JSON and we will
      // not show the raw json dialog pop up.
    }

    return (
      <Box
        key={`${index}list`}
        style={style}
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
            // Only open if user is not selecting text
            handleLogLineClick(formattedLogLine, message);
          }
        }}
      >
        {lowlight
          .highlight(language, message)
          .children.map((v) => value2react(v, index.toString(), keywords))}
        <br />
      </Box>
    );
  };

  useEffect(() => {
    const originContent = content.split("\n");
    if (timmer.current) {
      clearTimeout(timmer.current);
    }
    timmer.current = setTimeout(() => {
      setLogs(
        originContent
          .map((e, i) => ({
            i,
            origin: e,
            time: (e?.match(timeReg) || [""])[0],
          }))
          .filter((e) => {
            let bool = e.origin.includes(keywords);
            if (
              e.time &&
              startTime &&
              !dayjs(e.time).isAfter(dayjs(startTime))
            ) {
              bool = false;
            }
            if (e.time && endTime && !dayjs(e.time).isBefore(dayjs(endTime))) {
              bool = false;
            }
            return bool;
          })
          .map((e) => ({
            ...e,
          })),
      );
    }, 500);
  }, [content, keywords, language, startTime, endTime]);

  useEffect(() => {
    if (el.current) {
      el.current?.scrollTo((focusLine - 1) * (fontSize + 6));
    }
  }, [focusLine, fontSize]);

  useEffect(() => {
    let outterCurrentValue: any = null;
    if (outter.current) {
      const scrollFunc = (event: any) => {
        const { target } = event;
        if (
          target &&
          target.scrollTop + target.clientHeight === target.scrollHeight
        ) {
          if (onScrollBottom) {
            onScrollBottom(event);
          }
        }
        outterCurrentValue = outter.current;
      };
      outter.current.addEventListener("scroll", scrollFunc);
      return () => {
        if (outterCurrentValue) {
          outterCurrentValue.removeEventListener("scroll", scrollFunc);
        }
      };
    }
  }, [onScrollBottom]);

  return (
    <div>
      {logs && logs.length > MAX_LINES_FOR_LOGS && (
        <Box component="p" sx={{ color: (theme) => theme.palette.error.main }}>
          [Truncation warning] This log has been truncated and only the latest{" "}
          {MAX_LINES_FOR_LOGS} lines are displayed. Click "Download" button
          above to see the full log
        </Box>
      )}
      <List
        height={height || 600}
        width={width}
        ref={el}
        outerRef={outter}
        className={`hljs-${theme}`}
        style={{
          fontSize,
          fontFamily: "menlo, monospace",
          ...style,
        }}
        itemSize={fontSize + 6}
        itemCount={total}
      >
        {itemRenderer}
      </List>
      {selectedLogLine && (
        <LogLineDetailDialog
          formattedLogLine={selectedLogLine[0]}
          message={selectedLogLine[1]}
          onClose={() => {
            setSelectedLogLine(undefined);
          }}
        />
      )}
    </div>
  );
};

export default LogVirtualView;
