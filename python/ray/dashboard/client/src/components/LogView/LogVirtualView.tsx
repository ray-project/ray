import { alpha, Box, Typography } from "@mui/material";
import dayjs from "dayjs";
import prolog from "highlight.js/lib/languages/prolog";
import { lowlight } from "lowlight";
import React, {
  forwardRef,
  MutableRefObject,
  useCallback,
  useEffect,
  useImperativeHandle,
  useMemo,
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
  /** Search mode: "locate" shows all lines with navigation, "filter" shows only matching lines */
  searchMode?: "filter" | "locate";
  /** Callback when match info changes (total matches, current index) */
  onMatchInfoChange?: (info: { total: number; currentIndex: number }) => void;
};

export type LogVirtualViewHandle = {
  scrollToMatch: (matchIndex: number) => void;
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
                  bgcolor:
                    theme.palette.mode === "dark"
                      ? theme.palette.grey[900]
                      : theme.palette.grey[200],
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
              bgcolor:
                theme.palette.mode === "dark"
                  ? theme.palette.grey[900]
                  : theme.palette.grey[200],
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

const LogVirtualView = forwardRef<LogVirtualViewHandle, LogVirtualViewProps>(
  (
    {
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
      searchMode = "locate",
      onMatchInfoChange,
    },
    ref,
  ) => {
    const [logs, setLogs] = useState<{ i: number; origin: string }[]>([]);
    const [matchIndices, setMatchIndices] = useState<number[]>([]);
    const [currentMatchIndex, setCurrentMatchIndex] = useState<number>(-1);
    const total = logs.length;
    const timer = useRef<ReturnType<typeof setTimeout>>();
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

    // Create a set of matching line indices for O(1) lookup
    const matchIndexSet = useMemo(
      () => new Set(matchIndices),
      [matchIndices],
    );

    // Expose scrollToMatch via ref
    useImperativeHandle(ref, () => ({
      scrollToMatch: (matchIndex: number) => {
        if (matchIndex >= 0 && matchIndex < matchIndices.length) {
          setCurrentMatchIndex(matchIndex);
          const logIndex = matchIndices[matchIndex];
          if (el.current) {
            const listIndex = revert ? logs.length - 1 - logIndex : logIndex;
            el.current.scrollToItem(listIndex, "center");
          }
        }
      },
    }));

    // Notify parent when match info changes
    useEffect(() => {
      if (onMatchInfoChange) {
        onMatchInfoChange({
          total: matchIndices.length,
          currentIndex: currentMatchIndex,
        });
      }
    }, [matchIndices, currentMatchIndex, onMatchInfoChange]);

    const itemRenderer = ({ index, style }: { index: number; style: any }) => {
      const logIndex = revert ? logs.length - 1 - index : index;
      const { i, origin } = logs[logIndex];
      const isMatch = searchMode === "locate" && keywords && matchIndexSet.has(logIndex);
      const isCurrentMatch = isMatch && matchIndices[currentMatchIndex] === logIndex;

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
          sx={(theme) => ({
            // Explicitly create a stacking context so that the ::after pseudo-element
            // with z-index: -1 stays behind the text but in front of the list background.
            zIndex: 0,
            overflowX: "visible",
            whiteSpace: "nowrap",
            // Highlight the current match with a distinct background
            ...(isCurrentMatch && {
              backgroundColor: alpha(theme.palette.warning.main, 0.3),
            }),
            // Highlight other matches with a subtle background
            ...(isMatch && !isCurrentMatch && {
              backgroundColor: alpha(theme.palette.warning.main, 0.12),
            }),
            // Reveal the hidden button on row hover using a CSS-only selector.
            // This avoids React state changes on every mouse move, keeping
            // scroll and render performance smooth for large log files.
            "&:hover .log-line-details-btn": {
              opacity: 1,
              pointerEvents: "auto",
            },
            // Expand the row hover area to cover blank viewport space when
            // the log panel is horizontally scrolled.
            "&::after": {
              content: '""',
              position: "absolute",
              top: 0,
              right: "calc(-1 * var(--log-view-scroll-left, 0px))",
              width: "var(--log-view-scroll-left, 0px)",
              height: "100%",
              zIndex: -1,
            },
            "&::before": {
              content: `"${i + 1}"`,
              marginRight: 0.5,
              width: `${logs.length}`.length * 6 + 4,
              color: theme.palette.text.disabled,
              display: "inline-block",
            },
          })}
        >
          {lowlight
            .highlight(language, message)
            .children.map((v) => value2react(v, index.toString(), keywords))}
          {/* Only render the button for structured (JSON) log lines.
              Plain text lines have no additional data to show in the dialog. */}

          {formattedLogLine !== null && (
            <button
              className="log-line-details-btn"
              onClick={(e) => {
                e.stopPropagation();
                if ((window.getSelection()?.toString().length ?? 0) === 0) {
                  handleLogLineClick(formattedLogLine, message);
                }
              }}
            >
              Show details
            </button>
          )}
          <br />
        </Box>
      );
    };

    useEffect(() => {
      const originContent = content.split("\n");
      if (timer.current) {
        clearTimeout(timer.current);
      }
      timer.current = setTimeout(() => {
        const allLines = originContent.map((e, i) => ({
          i,
          origin: e,
          time: (startTime || endTime) ? (e?.match(timeReg) || [""])[0] : "",
        }));

        // Apply time filtering
        const timeFiltered = allLines.filter((e) => {
          let bool = true;
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
        });

        if (searchMode === "locate" || !keywords) {
          // Locate mode: show all lines, track which ones match
          const filtered = timeFiltered.map((e) => ({ i: e.i, origin: e.origin }));
          setLogs(filtered);

          // Compute match indices
          if (keywords) {
            const indices: number[] = [];
            filtered.forEach((line, idx) => {
              if (line.origin.includes(keywords)) {
                indices.push(idx);
              }
            });
            setMatchIndices(indices);
            // Reset to first match when keywords change
            setCurrentMatchIndex(indices.length > 0 ? 0 : -1);
            // Auto-scroll to first match
            if (indices.length > 0 && el.current) {
              const firstMatchIndex = indices[0];
              setTimeout(() => {
                const listIndex = revert ? filtered.length - 1 - firstMatchIndex : firstMatchIndex;
                el.current?.scrollToItem(listIndex, "center");
              }, 0);
            }
          } else {
            setMatchIndices([]);
            setCurrentMatchIndex(-1);
          }
        } else {
          // Filter mode: show only matching lines (original behavior)
          const filtered = timeFiltered
            .filter((e) => e.origin.includes(keywords))
            .map((e) => ({ i: e.i, origin: e.origin }));
          setLogs(filtered);
          setMatchIndices([]);
          setCurrentMatchIndex(-1);
        }
      }, 500);
    }, [content, keywords, language, startTime, endTime, searchMode]);

    useEffect(() => {
      if (el.current) {
        el.current?.scrollTo((focusLine - 1) * (fontSize + 6));
      }
    }, [focusLine, fontSize]);

    useEffect(() => {
      let outterCurrentValue: any = null;
      if (outter.current) {
        outter.current.style.setProperty("--log-view-scroll-left", "0px");

        const scrollFunc = (event: any) => {
          const { target } = event;
          if (target) {
            const scrollLeft = `${target.scrollLeft ?? 0}px`;
            if (
              target.style.getPropertyValue("--log-view-scroll-left") !==
              scrollLeft
            ) {
              target.style.setProperty("--log-view-scroll-left", scrollLeft);
            }
          }
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
          overscanCount={50}
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
  },
);

export default LogVirtualView;
