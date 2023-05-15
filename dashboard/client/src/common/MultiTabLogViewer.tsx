import {
  Box,
  createStyles,
  IconButton,
  makeStyles,
  Tab,
  Tabs,
  Typography,
} from "@material-ui/core";
import React, { useState } from "react";
import { RiExternalLinkLine, RiSortAsc, RiSortDesc } from "react-icons/ri";
import { Link } from "react-router-dom";
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
  nodeId: string | null;
  filename?: string;
};

export type MultiTabLogViewerProps = {
  tabs: MultiTabLogViewerTabDetails[];
  otherLogsLink?: string;
} & ClassNameProps;

export const MultiTabLogViewer = ({
  tabs,
  otherLogsLink,
  className,
}: MultiTabLogViewerProps) => {
  const classes = useStyles();
  const [value, setValue] = useState(tabs[0]?.title);
  const [expanded, setExpanded] = useState(false);

  const currentTab = tabs.find((tab) => tab.title === value);

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
          <Tabs
            className={classes.tabs}
            value={value}
            onChange={(_, newValue) => {
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
                  // Prevent the tab from changing
                  setValue(value);
                }}
                component={Link}
                to={otherLogsLink}
                target="_blank"
                rel="noopener noreferrer"
              />
            )}
          </Tabs>

          {!currentTab ? (
            <Typography color="error">Please select a tab.</Typography>
          ) : (
            tabs.map(({ title, nodeId, filename }) => (
              <HideableBlock
                key={title}
                visible={title === currentTab?.title}
                keepRendered
              >
                <StateApiLogViewer
                  nodeId={nodeId}
                  filename={filename}
                  height={expanded ? 800 : 300}
                />
              </HideableBlock>
            ))
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

export type StateApiLogViewerProps = {
  nodeId?: string | null;
  filename?: string;
  height?: number;
};

export const StateApiLogViewer = ({
  nodeId,
  filename,
  height = 300,
}: StateApiLogViewerProps) => {
  const { downloadUrl, log, path, refresh } = useStateApiLogs(nodeId, filename);
  return typeof log === "string" ? (
    <LogViewer
      log={log}
      path={path}
      downloadUrl={downloadUrl}
      height={height}
      onRefreshClick={() => {
        refresh();
      }}
    />
  ) : (
    <Typography color="error">Failed to load</Typography>
  );
};
