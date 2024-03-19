import {
  Button,
  createStyles,
  InputAdornment,
  LinearProgress,
  makeStyles,
  Switch,
  TextField,
} from "@material-ui/core";
import { SearchOutlined } from "@material-ui/icons";
import React, { useState } from "react";
import LogVirtualView from "../../components/LogView/LogVirtualView";

const useStyles = makeStyles((theme) =>
  createStyles({
    search: {
      margin: theme.spacing(1),
    },
  }),
);

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

export const LogViewer = ({
  path,
  log,
  downloadUrl,
  onRefreshClick,
  height = 600,
}: LogViewerProps) => {
  const classes = useStyles();

  const { search, setSearch, startTime, setStart, endTime, setEnd } =
    useLogViewer();

  return (
    <React.Fragment>
      {log !== "Loading..." && (
        <div>
          <div>
            <TextField
              className={classes.search}
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
              className={classes.search}
              label="Line Number"
              InputProps={{
                onChange: ({ target: { value } }) => {
                  setSearch({ ...search, lineNumber: value });
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
              className={classes.search}
              label="Font Size"
              InputProps={{
                onChange: ({ target: { value } }) => {
                  setSearch({ ...search, fontSize: Number(value) });
                },
                type: "",
              }}
            />
            <TextField
              id="datetime-local"
              label="Start Time"
              type="datetime-local"
              value={startTime}
              className={classes.search}
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
              className={classes.search}
              onChange={(val) => {
                setEnd(val.target.value);
              }}
              InputLabelProps={{
                shrink: true,
              }}
            />
            <div className={classes.search}>
              Reverse:{" "}
              <Switch
                checked={search?.revert}
                onChange={(e, v) => setSearch({ ...search, revert: v })}
              />
              {onRefreshClick && (
                <Button
                  className={classes.search}
                  variant="contained"
                  onClick={onRefreshClick}
                >
                  Refresh
                </Button>
              )}
              <Button
                className={classes.search}
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
            </div>
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
};
