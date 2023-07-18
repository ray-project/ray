import { Button, List, ListItem, makeStyles, Paper } from "@material-ui/core";
import React, { useEffect, useState } from "react";
import { Outlet, useLocation, useParams } from "react-router-dom";
import { SearchInput } from "../../components/SearchComponent";
import TitleCard from "../../components/TitleCard";
import { getLogDetail, getLogDownloadUrl } from "../../service/log";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { LogViewer } from "./LogViewer";

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

const useLogs = () => {
  const { search: urlSearch } = useLocation();
  const { host, path } = useParams();
  const searchMap = new URLSearchParams(urlSearch);
  const urlFileName = searchMap.get("fileName");
  const [origin, setOrigin] = useState<string>();
  const [fileName, setFileName] = useState(searchMap.get("fileName") || "");
  const [log, setLogs] =
    useState<undefined | string | { [key: string]: string }[]>();
  const [downloadUrl, setDownloadUrl] = useState<string>();

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
    setDownloadUrl(getLogDownloadUrl(url));
    getLogDetail(url)
      .then((res) => {
        if (res) {
          setLogs(res);
        } else {
          setLogs("(This file is empty.)");
        }
      })
      .catch(() => {
        setLogs("Failed to load");
      });
  }, [host, path]);

  return {
    log,
    origin,
    downloadUrl,
    host,
    path,
    fileName,
    setFileName,
  };
};

const Logs = () => {
  const classes = useStyles();
  const { log, origin, downloadUrl, path, fileName, setFileName } = useLogs();
  let href = "#/logs/";

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
    <div className={classes.root}>
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
                      href={`#/logs/${
                        origin ? `${encodeURIComponent(origin)}/` : ""
                      }${encodeURIComponent(e.href)}`}
                    >
                      {e.name}
                    </a>
                  </ListItem>
                ))}
            </List>
          )}
          {typeof log === "string" && (
            <LogViewer path={path} log={log} downloadUrl={downloadUrl} />
          )}
        </Paper>
      </TitleCard>
    </div>
  );
};

/**
 * Logs page for the new information architecture
 */
export const LogsLayout = () => {
  return (
    <React.Fragment>
      <MainNavPageInfo
        pageInfo={{ title: "Logs", id: "logs", path: "/logs" }}
      />
      <Outlet />
    </React.Fragment>
  );
};

export default Logs;
