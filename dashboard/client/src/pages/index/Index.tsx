import {
  Grid,
  makeStyles,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
} from "@material-ui/core";
import React, { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { version } from "../../../package.json";
import TitleCard from "../../components/TitleCard";
import { getRayConfig } from "../../service/cluster";
import { getNodeList } from "../../service/node";
import { getNamespaces } from "../../service/util";
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
  const [namespaces, setNamespaces] = useState<
    { namespaceId: string; hostNameList: string[] }[]
  >();
  const [nodes, setNodes] = useState<NodeDetail[]>([]);
  useEffect(() => {
    getRayConfig().then((res) => {
      if (res?.data?.data?.config) {
        setConfig(res.data.data.config);
      }
    });
  }, []);
  useEffect(() => {
    getNamespaces().then((res) => {
      if (res?.data?.data?.namespaces) {
        setNamespaces(res.data.data.namespaces);
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

  return { rayConfig, namespaces, nodes };
};

const Index = () => {
  const { rayConfig, namespaces, nodes } = useIndex();
  const classes = useStyle();

  return (
    <div className={classes.root}>
      <TitleCard title={rayConfig?.clusterName || "SUMMARY"}>
        <p>Dashboard Frontend Version: {version}</p>
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
      {namespaces && (
        <TitleCard title="Namespaces">
          {namespaces.map((namespace) => (
            <Grid>
              <p className={classes.label}>{namespace.namespaceId}</p>
              {namespace.hostNameList.map((host) => (
                <Link
                  key="host"
                  style={{ margin: 4 }}
                  to={`node/${
                    nodes.find(
                      (e) => e.hostname === host && e.raylet.state === "ALIVE",
                    )?.raylet.nodeId || ""
                  }`}
                >
                  {host}
                </Link>
              ))}
            </Grid>
          ))}
        </TitleCard>
      )}
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
