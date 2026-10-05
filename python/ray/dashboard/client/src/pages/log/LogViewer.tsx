import {
  KeyboardArrowDown,
  KeyboardArrowUp,
  SearchOutlined,
} from "@mui/icons-material";
import {
  Box,
  Button,
  IconButton,
  InputAdornment,
  LinearProgress,
  Switch,
  TextField,
  Tooltip,
  Typography,
} from "@mui/material";
import React, { memo, useCallback, useContext, useRef, useState } from "react";
import { GlobalContext } from "../../App";
import LogVirtualView, {
  LogVirtualViewHandle,
} from "../../components/LogView/LogVirtualView";

const useLogViewer = () => {
  const [search, setSearch] =
    useState<{
      keywords?: string;
      lineNumber?: string;
      fontSize?: number;
      revert?: boolean;
      searchMode?: "filter" | "locate";
    }>({ searchMode: "locate" });
  const [startTime, setStart] = useState<string>();
  const [endTime, setEnd] = useState<string>();
  const [matchInfo, setMatchInfo] = useState<{
    total: number;
    currentIndex: number;
  }>({ total: 0, currentIndex: -1 });

  return {
    search,
    setSearch,
    startTime,
    setStart,
    endTime,
    setEnd,
    matchInfo,
    setMatchInfo,
  };
};

type LogViewerProps = {
  path?: string;
  log: string;
  downloadUrl?: string;
  onRefreshClick?: () => void;
  height?: number;
};

/**
 * Memoized component because React-window does not work well with re-renders.
 * If text is selected, it will get unselected if the component re-renders.
 */
export const LogViewer = memo(
  ({
    path,
    log,
    downloadUrl,
    onRefreshClick,
    height = 600,
  }: LogViewerProps) => {
    const {
      search,
      setSearch,
      startTime,
      setStart,
      endTime,
      setEnd,
      matchInfo,
      setMatchInfo,
    } = useLogViewer();
    const { themeMode } = useContext(GlobalContext);
    const logViewRef = useRef<LogVirtualViewHandle>(null);

    const handlePrevMatch = useCallback(() => {
      if (matchInfo.total > 0 && logViewRef.current) {
        const newIndex =
          matchInfo.currentIndex <= 0
            ? matchInfo.total - 1
            : matchInfo.currentIndex - 1;
        logViewRef.current.scrollToMatch(newIndex);
      }
    }, [matchInfo]);

    const handleNextMatch = useCallback(() => {
      if (matchInfo.total > 0 && logViewRef.current) {
        const newIndex =
          matchInfo.currentIndex >= matchInfo.total - 1
            ? 0
            : matchInfo.currentIndex + 1;
        logViewRef.current.scrollToMatch(newIndex);
      }
    }, [matchInfo]);

    return (
      <React.Fragment>
        {log !== "Loading..." && (
          <div>
            <div>
              <TextField
                sx={{ margin: 1 }}
                label="Keyword"
                InputProps={{
                  onChange: ({ target: { value } }) => {
                    setSearch({ ...search, keywords: value });
                  },
                  onKeyDown: (e) => {
                    if (e.key === "Enter") {
                      e.preventDefault();
                      if (e.shiftKey) {
                        handlePrevMatch();
                      } else {
                        handleNextMatch();
                      }
                    }
                  },
                  type: "",
                  endAdornment: (
                    <InputAdornment position="end">
                      <SearchOutlined
                        sx={(theme) => ({
                          color: theme.palette.text.secondary,
                        })}
                      />
                    </InputAdornment>
                  ),
                }}
              />
              {/* Search mode navigation controls */}
              {search?.searchMode === "locate" && search?.keywords && (
                <Box
                  sx={{
                    display: "inline-flex",
                    alignItems: "center",
                    margin: 1,
                    gap: 0.5,
                  }}
                >
                  <Typography variant="body2" sx={{ minWidth: 60 }}>
                    {matchInfo.total > 0
                      ? `${matchInfo.currentIndex + 1} / ${matchInfo.total}`
                      : "0 / 0"}
                  </Typography>
                  <Tooltip title="Previous match (Shift+Enter)">
                    <span>
                      <IconButton
                        size="small"
                        onClick={handlePrevMatch}
                        disabled={matchInfo.total === 0}
                      >
                        <KeyboardArrowUp />
                      </IconButton>
                    </span>
                  </Tooltip>
                  <Tooltip title="Next match (Enter)">
                    <span>
                      <IconButton
                        size="small"
                        onClick={handleNextMatch}
                        disabled={matchInfo.total === 0}
                      >
                        <KeyboardArrowDown />
                      </IconButton>
                    </span>
                  </Tooltip>
                </Box>
              )}
              <TextField
                id="datetime-local"
                label="Start Time"
                type="datetime-local"
                value={startTime}
                sx={{ margin: 1 }}
                onChange={(val) => {
                  setStart(val.target.value);
                }}
                InputLabelProps={{
                  shrink: true,
                }}
              />
              <TextField
                label="End Time"
                type="datetime-local"
                value={endTime}
                sx={{ margin: 1 }}
                onChange={(val) => {
                  setEnd(val.target.value);
                }}
                InputLabelProps={{
                  shrink: true,
                }}
              />
              <Box sx={{ margin: 1 }}>
                Filter Mode:{" "}
                <Tooltip title="Filter mode shows only matching lines. Locate mode shows all lines with navigation.">
                  <Switch
                    checked={search?.searchMode === "filter"}
                    onChange={(_, checked) =>
                      setSearch({
                        ...search,
                        searchMode: checked ? "filter" : "locate",
                      })
                    }
                  />
                </Tooltip>
                Reverse:{" "}
                <Switch
                  checked={search?.revert}
                  onChange={(_, checked) =>
                    setSearch({ ...search, revert: checked })
                  }
                />
                {onRefreshClick && (
                  <Button
                    sx={{ margin: 1 }}
                    variant="contained"
                    onClick={onRefreshClick}
                  >
                    Refresh
                  </Button>
                )}
                <Button
                  sx={{ margin: 1 }}
                  variant="contained"
                  onClick={() => {
                    setStart("");
                    setEnd("");
                  }}
                >
                  Reset Time
                </Button>
                {downloadUrl && path && (
                  <Button
                    variant="contained"
                    component="a"
                    href={downloadUrl}
                    download={path}
                  >
                    Download log file
                  </Button>
                )}
              </Box>
            </div>
            <LogVirtualView
              ref={logViewRef}
              height={height}
              revert={search?.revert}
              keywords={search?.keywords}
              focusLine={Number(search?.lineNumber) || undefined}
              fontSize={search?.fontSize || 12}
              content={log}
              language="prolog"
              theme={themeMode}
              startTime={startTime}
              endTime={endTime}
              searchMode={search?.searchMode}
              onMatchInfoChange={setMatchInfo}
            />
          </div>
        )}
        {log === "Loading..." && (
          <div>
            <br />
            <LinearProgress />
          </div>
        )}
      </React.Fragment>
    );
  },
);
