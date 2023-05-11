import { Box, IconButton, Tab, Tabs, Typography } from "@material-ui/core";
import React, { useState } from "react";
import { RiArrowUpDownLine } from "react-icons/ri";
import { useStateApiLogs } from "../pages/log/hooks";
import { LogViewer } from "../pages/log/LogViewer";
import { HideableBlock } from "./CollapsibleSection";

export type MultiTabLogViewerTab = {
  title: string;
  nodeId: string | null;
  filename?: string;
};

export type MultiTabLogViewerProps = {
  tabs: MultiTabLogViewerTab[];
};

export const MultiTabLogViewer = ({ tabs }: MultiTabLogViewerProps) => {
  const [value, setValue] = useState(tabs[0]?.title);
  const [expanded, setExpanded] = useState(false);

  const currentTab = tabs.find((tab) => tab.title === value);

  return (
    <div>
      <Box
        display="flex"
        flexDirection="row"
        alignItems="center"
        justifyContent="space-between"
      >
        <Tabs
          value={value}
          onChange={(_, newValue) => {
            setValue(newValue);
          }}
        >
          {tabs.map(({ title }) => (
            <Tab key={title} label={title} value={title} />
          ))}
        </Tabs>
        <IconButton
          onClick={() => {
            setExpanded(!expanded);
          }}
        >
          <RiArrowUpDownLine />
        </IconButton>
      </Box>
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
