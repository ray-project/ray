import { Tooltip } from "@material-ui/core";
import { makeStyles } from "@material-ui/core/styles";
import React, { useEffect, useState } from "react";
import { getClusterMetadata } from "../service/global";

const useStyles = makeStyles((theme) => ({
    overflowCell: {
      display: "block",
      width: "150px",
      textOverflow: "ellipsis",
      overflow: "hidden",
      whiteSpace: "nowrap",
    },
  }));

export const ClusterMetadata = () => {
  const [rayVersion, setRayVersion] = useState("");
  const [pythonVersion, setPythonVersion] = useState("");
  const [sessionId, setSessionId] = useState("");
  const [gitCommit, setGitCommit] = useState("");
  const [os, setOs] = useState("");
  const classes = useStyles();
  useEffect(() => {
    getClusterMetadata().then(({ data }) => {
      console.log(data)
      console.log(data.data.rayVersion)
      setRayVersion(data.data.rayVersion);
      setPythonVersion(data.data.pythonVersion);
      setSessionId(data.data.sessionId);
      setGitCommit(data.data.gitCommit);
      setOs(data.data.os);
    });
  }, []);

  return  <div>
    <Tooltip
        className={classes.overflowCell}
        title={rayVersion}
        arrow
        interactive
    >
        <div><b>Ray Version</b>: {rayVersion}</div>
    </Tooltip>
    <Tooltip
        className={classes.overflowCell}
        title={pythonVersion}
        arrow
        interactive
    >
        <div><b>Python Version</b>: {pythonVersion}</div>
    </Tooltip>
    <Tooltip
        className={classes.overflowCell}
        title={gitCommit}
        arrow
        interactive
    >
        <div><b>Ray Commit</b>: {gitCommit}</div>
    </Tooltip>
    <Tooltip
        className={classes.overflowCell}
        title={os}
        arrow
        interactive
    >
        <div><b>OS</b>: {os}</div>
    </Tooltip>
    <Tooltip
        className={classes.overflowCell}
        title={sessionId}
        arrow
        interactive
    >
        <div><b>Session ID</b>: {sessionId}</div>
    </Tooltip>
  </div>
};
