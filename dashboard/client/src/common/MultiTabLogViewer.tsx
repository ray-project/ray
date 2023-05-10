import { Tab, Tabs, Typography } from "@material-ui/core";
import React, { useState } from "react";
import { useStateApiLogs } from "../pages/log/hooks";
import { LogViewer } from "../pages/log/LogViewer";
import { HideableBlock } from "./CollapsibleSection";

export type MultiTabLogViewerTab = {
  title: string;
  nodeId: string;
  filename: string;
};

export type MultiTabLogViewerProps = {
  tabs: MultiTabLogViewerTab[];
};

export const MultiTabLogViewer = ({ tabs }: MultiTabLogViewerProps) => {
  const [value, setValue] = useState(tabs[0]?.title);

  const currentTab = tabs.find((tab) => tab.title === value);

  return (
    <div>
      <Tabs
        value={value}
        onChange={(event, newValue) => {
          setValue(newValue);
        }}
      >
        {tabs.map(({ title }) => (
          <Tab key={title} label={title} value={title} />
        ))}
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
            <StateApiLogViewer nodeId={nodeId} filename={filename} />
          </HideableBlock>
        ))
      )}
    </div>
  );
};

export type StateApiLogViewerProps = {
  nodeId?: string | null;
  filename?: string;
};

export const StateApiLogViewer = ({
  nodeId,
  filename,
}: StateApiLogViewerProps) => {
  const { downloadUrl, log, path, refresh } = useStateApiLogs(nodeId, filename);
  return typeof log === "string" ? (
    <LogViewer
      log={log}
      path={path}
      downloadUrl={downloadUrl}
      height={300}
      onRefreshClick={() => {
        refresh();
      }}
    />
  ) : (
    <Typography color="error">Failed to load</Typography>
  );
};
