import { Tab, Tabs, Typography } from "@material-ui/core";
import React, { useState } from "react";
import { useStateApiLogs } from "../pages/log/hooks";
import { LogViewer } from "../pages/log/LogViewer";

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

  const { downloadUrl, log, path, refresh } = useStateApiLogs(
    currentTab?.nodeId,
    currentTab?.filename,
  );

  return (
    <div>
      <Tabs
        value={value}
        onChange={(event, newValue) => {
          setValue(newValue);
        }}
      >
        {tabs.map(({ title }) => (
          <Tab label={title} value={title} />
        ))}
      </Tabs>
      {!currentTab ? (
        <Typography color="error">Please select a tab.</Typography>
      ) : typeof log !== "string" ? (
        <Typography color="error">Failed to load</Typography>
      ) : (
        <LogViewer
          log={log}
          path={path}
          downloadUrl={downloadUrl}
          height={300}
          onRefreshClick={() => {
            refresh();
          }}
        />
      )}
    </div>
  );
};
