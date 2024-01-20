import {
  Box,
  createStyles,
  IconButton,
  makeStyles,
  Tab,
  Tabs,
  Typography,
} from "@material-ui/core";
import React, { useEffect, useState } from "react";
import { RiExternalLinkLine, RiSortAsc, RiSortDesc } from "react-icons/ri";
import { Link } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";
import { useStateApiLogs } from "../pages/log/hooks";
import { LogViewer } from "../pages/log/LogViewer";
import { HideableBlock } from "./CollapsibleSection";
import { ClassNameProps } from "./props";

const useStyles = makeStyles((theme) =>
  createStyles({
    tabs: {
      borderBottom: `1px solid ${theme.palette.divider}`,
    },
  }),
);

export type MultiTabLogViewerTabDetails = {
  title: string;
} & LogViewerData;

export type MultiTabLogViewerProps = {
  tabs: MultiTabLogViewerTabDetails[];
  otherLogsLink?: string;
  /**
   * If set, this multi-tab log viewer will remember the last selected tab and start on that tab
   * the next time this component is rendered.
   *
   * Different string values to provide different contexts for this memory. For example, if you
   * want all multi-tab log viewers in the actor detail page to share one memory, they should have
   * the same string value here.
   */
  contextKey?: string;
} & ClassNameProps;

export const MultiTabLogViewer = ({
  tabs,
  otherLogsLink,
  contextKey,
  className,
}: MultiTabLogViewerProps) => {
  const classes = useStyles();

  // DO NOT use `cachedTab` or `setCachedTab` when `contextKey` is undefined!
  const [cachedTab, setCachedTab] = useLocalStorage(
    `MultiTabLogViewer-tabMemory-${contextKey}`,
    null,
  );

  const [value, setValue] = useState(
    contextKey && cachedTab ? cachedTab : tabs[0]?.title,
  );
  const [expanded, setExpanded] = useState(false);

  useEffect(() => {
    // If current tab value is not valid, reset to first tab.
    if (!tabs.some((tab) => tab.title === value)) {
      setValue(tabs[0]?.title);
    }
  }, [tabs, value]);

  const currentTab = tabs.find((tab) => tab.title === value);

  if (tabs.length === 0) {
    return <Typography>No logs to display.</Typography>;
  }

  return (
    <div className={className}>
      <Box
        display="flex"
        flexDirection="row"
        alignItems="flex-start"
        justifyContent="space-between"
      >
        <Box
          display="flex"
          flexDirection="column"
          alignItems="stretch"
          flexGrow={1}
        >
          {(tabs.length > 1 || otherLogsLink) && (
            <Tabs
              className={classes.tabs}
              value={value}
              onChange={(_, newValue) => {
                if (contextKey) {
                  setCachedTab(newValue);
                }
                setValue(newValue);
              }}
              indicatorColor="primary"
            >
              {tabs.map(({ title }) => (
                <Tab key={title} label={title} value={title} />
              ))}
              {otherLogsLink && (
                <Tab
                  label={
                    <Box display="flex" alignItems="center">
                      Other logs &nbsp; <RiExternalLinkLine size={20} />
                    </Box>
                  }
                  onClick={(event) => {
                    // Prevent the tab from changing by setting value to the current value
                    setValue(value);
                  }}
                  component={Link}
                  to={otherLogsLink}
                  target="_blank"
                  rel="noopener noreferrer"
                />
              )}
            </Tabs>
          )}

          {!currentTab ? (
            <Typography color="error">Please select a tab.</Typography>
          ) : (
            tabs.map((tab) => {
              const { title, ...data } = tab;
              return (
                <HideableBlock
                  key={title}
                  visible={title === currentTab?.title}
                  keepRendered
                >
                  <StateApiLogViewer
                    data={data}
                    height={expanded ? 800 : 300}
                  />
                </HideableBlock>
              );
            })
          )}
        </Box>
        <IconButton
          onClick={() => {
            setExpanded(!expanded);
          }}
        >
          {expanded ? <RiSortAsc /> : <RiSortDesc />}
        </IconButton>
      </Box>
    </div>
  );
};

type TextData = {
  contents: string;
};
type FileData = {
  nodeId: string | null;
  filename?: string;
};
type ActorData = {
  actorId: string | null;
  suffix: "out" | "err";
};
type TaskData = {
  taskId: string | null;
  suffix: "out" | "err";
};

type LogViewerData = TextData | FileData | ActorData | TaskData;

const isLogViewerDataText = (data: LogViewerData): data is TextData =>
  "contents" in data;

const isLogViewerDataActor = (data: LogViewerData): data is ActorData =>
  "actorId" in data;

const isLogViewerDataTask = (data: LogViewerData): data is TaskData =>
  "taskId" in data;

export type StateApiLogViewerProps = {
  height?: number;
  data: LogViewerData;
};

export const StateApiLogViewer = ({
  height = 300,
  data,
}: StateApiLogViewerProps) => {
  if (isLogViewerDataText(data)) {
    return <TextLogViewer height={height} contents={data.contents} />;
  } else if (isLogViewerDataActor(data)) {
    return <ActorLogViewer height={height} {...data} />;
  } else if (isLogViewerDataTask(data)) {
    return <TaskLogViewer height={height} {...data} />;
  } else {
    return <FileLogViewer height={height} {...data} />;
  }
};

const TextLogViewer = ({
  height = 300,
  contents,
}: {
  height: number;
  contents: string;
}) => {
  return <LogViewer log={contents} height={height} />;
};

const FileLogViewer = ({
  height = 300,
  nodeId,
  filename,
}: {
  height: number;
} & FileData) => {
  const apiData = useStateApiLogs({ nodeId, filename }, filename);
  return <ApiLogViewer apiData={apiData} height={height} />;
};

const ActorLogViewer = ({
  height = 300,
  actorId,
  suffix,
}: {
  height: number;
} & ActorData) => {
  const apiData = useStateApiLogs(
    { actorId, suffix },
    `actor-log-${actorId}.${suffix}`,
  );
  return <ApiLogViewer apiData={apiData} height={height} />;
};

const TaskLogViewer = ({
  height = 300,
  taskId,
  suffix,
}: {
  height: number;
} & TaskData) => {
  const apiData = useStateApiLogs(
    { taskId, suffix },
    `task-log-${taskId}.${suffix}`,
  );
  return <ApiLogViewer apiData={apiData} height={height} />;
};

const ApiLogViewer = ({
  apiData: { downloadUrl, log, path, refresh },
  height = 300,
}: {
  apiData: ReturnType<typeof useStateApiLogs>;
  height: number;
}) => {
  return typeof log === "string" ? (
    <LogViewer
      log={log}
      path={path}
      downloadUrl={downloadUrl !== null ? downloadUrl : undefined}
      height={height}
      onRefreshClick={() => {
        refresh();
      }}
    />
  ) : (
    <Typography color="error">Failed to load</Typography>
  );
};
