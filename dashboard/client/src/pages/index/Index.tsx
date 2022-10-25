import {
  makeStyles,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
} from "@material-ui/core";
import React, { useEffect, useState } from "react";
import packageInfo from "../../../package.json";
import TitleCard from "../../components/TitleCard";
import { getRayConfig } from "../../service/cluster";
import { getNodeList } from "../../service/node";
import { RayConfig } from "../../type/config";
import { NodeDetail } from "../../type/node";
import { memoryConverter } from "../../util/converter";

const useStyle = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(2),
  },
  label: {
    fontWeight: "bold",
  },
}));

const getVal = (key: string, value: any) => {
  if (key === "containerMemory") {
    return memoryConverter(value * 1024 * 1024);
  }
  return JSON.stringify(value);
};

const useIndex = () => {
  const [rayConfig, setConfig] = useState<RayConfig>();
  const [nodes, setNodes] = useState<NodeDetail[]>([]);
  useEffect(() => {
    getRayConfig().then((res) => {
      if (res?.data?.data?.config) {
        setConfig(res.data.data.config);
      }
    });
  }, []);
  useEffect(() => {
    getNodeList().then((res) => {
      if (res?.data?.data?.summary) {
        setNodes(res.data.data.summary);
      }
    });
  }, []);

  return { rayConfig, nodes };
};

const Index = () => {
  const { rayConfig } = useIndex();
  const classes = useStyle();

  return (
    <div className={classes.root}>
      <TitleCard title={rayConfig?.clusterName || "SUMMARY"}>
        <p>Dashboard Frontend Version: {packageInfo.version}</p>
        {rayConfig?.imageUrl && (
          <p>
            Image Url:{" "}
            <a
              href={rayConfig.imageUrl}
              target="_blank"
              rel="noopener noreferrer"
            >
              {rayConfig.imageUrl}
            </a>
          </p>
        )}
        {rayConfig?.sourceCodeLink && (
          <p>
            Source Code:{" "}
            <a
              href={rayConfig.sourceCodeLink}
              target="_blank"
              rel="noopener noreferrer"
            >
              {rayConfig.sourceCodeLink}
            </a>
          </p>
        )}
      </TitleCard>
      {rayConfig && (
        <TitleCard title="Config">
          <TableContainer>
            <TableHead>
              <TableCell>Key</TableCell>
              <TableCell>Value</TableCell>
            </TableHead>
            <TableBody>
              {Object.entries(rayConfig).map(([key, value]) => (
                <TableRow>
                  <TableCell className={classes.label}>{key}</TableCell>
                  <TableCell>{getVal(key, value)}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </TableContainer>
        </TitleCard>
      )}
    </div>
  );
};

export default Index;
