import {
  Button,
  InputAdornment,
  LinearProgress,
  List,
  ListItem,
  makeStyles,
  Paper,
  Switch,
  TextField,
} from "@material-ui/core";
import { SearchOutlined } from "@material-ui/icons";
import React, { useEffect, useRef, useState } from "react";
import { Outlet, useLocation, useParams } from "react-router-dom";
import LogVirtualView from "../../components/LogView/LogVirtualView";
import { SearchInput } from "../../components/SearchComponent";
import TitleCard from "../../components/TitleCard";
import { getLogDetail } from "../../service/log";
import { MainNavPageInfo } from "../layout/mainNavContext";

const useStyles = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(2),
    width: "100%",
  },
  table: {
    marginTop: theme.spacing(4),
    padding: theme.spacing(2),
  },
  pageMeta: {
    padding: theme.spacing(2),
    marginTop: theme.spacing(2),
  },
  search: {
    margin: theme.spacing(1),
  },
}));

type LogsProps = {
  theme?: "dark" | "light";
  newIA?: boolean;
};

const useLogs = ({ theme, newIA }: LogsProps) => {
  const { search: urlSearch } = useLocation();
  const { host, path } = useParams();
  console.log("Host: " + host);
  console.log("Path: " + path);
  const searchMap = new URLSearchParams(urlSearch);
  const urlFileName = searchMap.get("fileName");
  const el = useRef<HTMLDivElement>(null);
  const [origin, setOrigin] = useState<string>();
  const [search, setSearch] =
    useState<{
      keywords?: string;
      lineNumber?: string;
      fontSize?: number;
      revert?: boolean;
    }>();
  const [fileName, setFileName] = useState(searchMap.get("fileName") || "");
  const [log, setLogs] =
    useState<undefined | string | { [key: string]: string }[]>();
  const [startTime, setStart] = useState<string>();
  const [endTime, setEnd] = useState<string>();

  useEffect(() => {
    setFileName(urlFileName || "");
  }, [urlFileName]);

  useEffect(() => {
    let url = "log_index";
    setLogs("Loading...");
    if (host) {
      url = decodeURIComponent(host);
      setOrigin(new URL(url).origin);
      if (path) {
        url += decodeURIComponent(path);
      }
    } else {
      setOrigin(undefined);
    }
    getLogDetail(url)
      .then((res) => {
        if (res) {
          setLogs(res);
        } else {
          setLogs("(null)");
        }
      })
      .catch(() => {
        setLogs("Failed to load");
      });
  }, [host, path]);

  return {
    log,
    origin,
    host,
    path,
    el,
    search,
    setSearch,
    theme,
    fileName,
    setFileName,
    startTime,
    setStart,
    endTime,
    setEnd,
  };
};

const Logs = (props: LogsProps) => {
  const classes = useStyles();
  const {
    log,
    origin,
    path,
    el,
    search,
    setSearch,
    theme,
    fileName,
    setFileName,
    startTime,
    setStart,
    endTime,
    setEnd,
  } = useLogs(props);
  const { newIA } = props;
  let href = newIA ? "#/new/log/" : "#/log/";

  if (origin) {
    if (path) {
      const after = decodeURIComponent(path).split("/");
      after.pop();
      if (after.length > 1) {
        href += encodeURIComponent(origin);
        href += "/";
        href += encodeURIComponent(after.join("/"));
      }
    }
  }

  return (
    <div className={classes.root} ref={el}>
      <TitleCard title="Logs Viewer">
        <Paper>
          {!origin && <p>Select a node to view logs</p>}
          {origin && (
            <p>
              Node: {origin}
              {decodeURIComponent(path || "")}
            </p>
          )}
          {origin && (
            <div>
              <Button
                variant="contained"
                href={href}
                className={classes.search}
              >
                Back To ../
              </Button>
              {typeof log === "object" && (
                <SearchInput
                  defaultValue={fileName}
                  label="File Name"
                  onChange={(val) => {
                    setFileName(val);
                  }}
                />
              )}
            </div>
          )}
        </Paper>
        <Paper>
          {typeof log === "object" && (
            <List>
              {log
                .filter((e) => !fileName || e?.name?.includes(fileName))
                .map((e: { [key: string]: string }) => (
                  <ListItem key={e.name}>
                    <a
                      href={
                        newIA
                          ? `#/new/logs/${
                              origin ? `${encodeURIComponent(origin)}/` : ""
                            }${encodeURIComponent(e.href)}`
                          : `#/log/${
                              origin ? `${encodeURIComponent(origin)}/` : ""
                            }${encodeURIComponent(e.href)}`
                      }
                    >
                      {e.name}
                    </a>
                  </ListItem>
                ))}
            </List>
          )}
          {typeof log === "string" && log !== "Loading..." && (
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
                </div>
              </div>
              <LogVirtualView
                height={600}
                theme={theme}
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
        </Paper>
      </TitleCard>
    </div>
  );
};

/**
 * Logs page for the new information architecture
 */
export const NewIALogsPage = () => {
  return (
    <React.Fragment>
      <MainNavPageInfo
        pageInfo={{ title: "Logs", id: "logs", path: "/new/logs" }}
      />
      <Outlet />
    </React.Fragment>
  );
};

export default Logs;
