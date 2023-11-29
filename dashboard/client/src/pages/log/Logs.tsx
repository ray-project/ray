import {
  Button,
  List,
  ListItem,
  makeStyles,
  Paper,
  Typography,
} from "@material-ui/core";
import React, { useState } from "react";
import { Link, Outlet, useSearchParams } from "react-router-dom";
import useSWR from "swr";
import { StateApiLogViewer } from "../../common/MultiTabLogViewer";
import { SearchInput } from "../../components/SearchComponent";
import TitleCard from "../../components/TitleCard";
import { listStateApiLogs } from "../../service/log";
import { getNodeList } from "../../service/node";
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

export const StateApiLogsListPage = () => {
  const classes = useStyles();
  const [searchParams] = useSearchParams();
  const nodeId = searchParams.get("nodeId");
  const folder = searchParams.get("folder");
  const fileNameParam = searchParams.get("fileName");
  const [fileName, setFileName] = useState(fileNameParam || "");

  const backFolder = folder
    ? [...folder.split("/").slice(0, -1)].join("/")
    : undefined;
  const backHref =
    // backGlob is undefined when glob is empty
    // backGlob is empty string when glob is 1 level deep.
    backFolder !== undefined && nodeId
      ? `/logs/?nodeId=${encodeURIComponent(
          nodeId,
        )}&folder=${encodeURIComponent(backFolder)}`
      : `/logs/`;

  return (
    <div className={classes.root}>
      <TitleCard title="Logs Viewer">
        <Paper>
          {!nodeId && <p>Select a node to view logs</p>}
          {nodeId && (
            <React.Fragment>
              <p>Node: {nodeId}</p>
              <p>{decodeURIComponent(folder || "")}</p>
            </React.Fragment>
          )}
          {nodeId && (
            <div>
              <Button
                component={Link}
                variant="contained"
                to={backHref}
                className={classes.search}
              >
                Back To ../
              </Button>
              <SearchInput
                defaultValue={fileName}
                label="File Name"
                onChange={(val) => {
                  setFileName(val);
                }}
              />
            </div>
          )}
        </Paper>
        <Paper>
          {nodeId ? (
            <StateApiLogsFilesList
              nodeId={nodeId}
              folder={folder}
              fileName={fileName}
            />
          ) : (
            <StateApiLogsNodesList />
          )}
        </Paper>
      </TitleCard>
    </div>
  );
};

export const StateApiLogsNodesList = () => {
  const { data: nodes, error } = useSWR(["/api/v0/nodes"], async () => {
    const resp = await getNodeList();
    const nodes = resp.data.data.summary;
    return nodes.filter((node) => node.raylet.state === "ALIVE");
  });

  const isLoading = nodes === undefined && error === undefined;

  return (
    <React.Fragment>
      {isLoading ? (
        <Typography>Loading...</Typography>
      ) : error ? (
        <Typography color="error">{error}</Typography>
      ) : (
        <List>
          {nodes?.map(({ raylet: { nodeId }, ip }) => (
            <ListItem key={nodeId}>
              <Link to={`?nodeId=${nodeId}`}>
                Node ID: {nodeId} (IP: {ip})
              </Link>
            </ListItem>
          ))}
        </List>
      )}
    </React.Fragment>
  );
};

type StateApiLogsFilesListProps = {
  nodeId: string;
  folder: string | null;
  fileName: string;
};

export const StateApiLogsFilesList = ({
  nodeId,
  folder,
  fileName,
}: StateApiLogsFilesListProps) => {
  // We want to do a partial search for file name.
  const fileNameGlob = fileName ? `*${fileName}*` : undefined;
  const glob = fileNameGlob
    ? folder
      ? `${folder}/${fileNameGlob}`
      : `${fileNameGlob}`
    : folder
    ? `${folder}/*`
    : undefined;

  const { data: fileGroups, error } = useSWR(
    nodeId ? ["/api/v0/logs", nodeId, glob] : null,
    async ([_, nodeId, glob]) => {
      const resp = await listStateApiLogs({ nodeId, glob });
      return resp.data.data.result;
    },
  );

  const isLoading = fileGroups === undefined && error === undefined;
  const files =
    fileGroups !== undefined
      ? Object.values(fileGroups)
          .flatMap((e) => e)
          .sort()
      : [];

  return (
    <React.Fragment>
      {isLoading ? (
        <p>Loading...</p>
      ) : (
        files.length === 0 && <p>No files found.</p>
      )}
      {files && (
        <List>
          {files.map((fileName) => {
            const isDir = fileName.endsWith("/");
            const fileNameWithoutEndingSlash = fileName.substring(
              0,
              fileName.length - 1,
            );
            const parentFolder = folder ? `${folder}/` : "";
            const fileNameWithoutParent = fileName.startsWith(parentFolder)
              ? fileName.substring(parentFolder.length)
              : fileName;

            return (
              <ListItem key={fileName}>
                <Link
                  to={
                    isDir
                      ? `?nodeId=${encodeURIComponent(
                          nodeId,
                        )}&folder=${encodeURIComponent(
                          fileNameWithoutEndingSlash,
                        )}`
                      : `viewer?nodeId=${encodeURIComponent(
                          nodeId,
                        )}&fileName=${encodeURIComponent(fileName)}`
                  }
                >
                  {fileNameWithoutParent}
                </Link>
              </ListItem>
            );
          })}
        </List>
      )}
    </React.Fragment>
  );
};

export const StateApiLogViewerPage = () => {
  const classes = useStyles();
  const [searchParams] = useSearchParams();
  const nodeId = searchParams.get("nodeId");
  const fileName = searchParams.get("fileName");

  const backFolder = fileName
    ? [...fileName.split("/").slice(0, -1)].join("/")
    : undefined;
  const backHref =
    // backGlob is undefined when glob is empty
    // backGlob is empty string when glob is 1 level deep.
    backFolder !== undefined
      ? `/logs/?nodeId=${nodeId}&folder=${backFolder}`
      : `/logs/?nodeId=${nodeId}`;

  return (
    <div className={classes.root}>
      <TitleCard title="Logs Viewer">
        <Paper>
          {!nodeId && <p>Select a node to view logs</p>}
          {nodeId && (
            <React.Fragment>
              <p>Node: {nodeId}</p>
              <p>File: {decodeURIComponent(fileName || "")}</p>
            </React.Fragment>
          )}
          {nodeId && (
            <div>
              <Button
                component={Link}
                variant="contained"
                to={backHref}
                className={classes.search}
              >
                Back To ../
              </Button>
            </div>
          )}
        </Paper>
        <Paper>
          {nodeId && fileName ? (
            <StateApiLogViewer
              data={{
                nodeId,
                filename: fileName,
              }}
              height={600}
            />
          ) : (
            <Typography color="error">Invalid url parameters</Typography>
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
