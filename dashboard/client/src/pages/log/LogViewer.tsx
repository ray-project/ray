import { SearchOutlined } from "@mui/icons-material";
import {
  Box,
  Button,
  InputAdornment,
  LinearProgress,
  Switch,
  TextField,
} from "@mui/material";
import React, { memo, useState } from "react";
import LogVirtualView from "../../components/LogView/LogVirtualView";

const useLogViewer = () => {
  const [search, setSearch] =
    useState<{
      keywords?: string;
      lineNumber?: string;
      fontSize?: number;
      revert?: boolean;
    }>();
  const [startTime, setStart] = useState<string>();
  const [endTime, setEnd] = useState<string>();

  return {
    search,
    setSearch,
    startTime,
    setStart,
    endTime,
    setEnd,
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
    const { search, setSearch, startTime, setStart, endTime, setEnd } =
      useLogViewer();

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
                  type: "",
                  endAdornment: (
                    <InputAdornment position="end">
                      <SearchOutlined />
                    </InputAdornment>
                  ),
                }}
              />
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
                Reverse:{" "}
                <Switch
                  checked={search?.revert}
                  onChange={(e, v) => setSearch({ ...search, revert: v })}
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
              height={height}
              revert={search?.revert}
              keywords={search?.keywords}
              focusLine={Number(search?.lineNumber) || undefined}
              fontSize={search?.fontSize || 12}
              content={log}
              language="prolog"
              startTime={startTime}
              endTime={endTime}
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
