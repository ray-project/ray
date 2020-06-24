import {
  createStyles,
  makeStyles,
  TableCell,
  TableRow,
  Theme,
} from "@material-ui/core";
import AddIcon from "@material-ui/icons/Add";
import RemoveIcon from "@material-ui/icons/Remove";
import classNames from "classnames";
import React, { useState } from "react";
import {
  NodeInfoResponse,
  NodeInfoResponseWorker,
  RayletInfoResponse,
} from "../../../api";
import { NodeCPU, WorkerCPU } from "./features/CPU";
import { NodeDisk, WorkerDisk } from "./features/Disk";
import { makeNodeErrors, makeWorkerErrors } from "./features/Errors";
import { NodeGPU, WorkerGPU } from "./features/GPU";
import { NodeGRAM, WorkerGRAM } from "./features/GRAM";
import { NodeHost, WorkerHost } from "./features/Host";
import { makeNodeLogs, makeWorkerLogs } from "./features/Logs";
import { NodeRAM, WorkerRAM } from "./features/RAM";
import { NodeReceived, WorkerReceived } from "./features/Received";
import { NodeSent, WorkerSent } from "./features/Sent";
import { NodeUptime, WorkerUptime } from "./features/Uptime";
import { NodeWorkers, WorkerWorkers } from "./features/Workers";

const useNodeRowGroupStyles = makeStyles((theme: Theme) =>
  createStyles({
    cell: {
      padding: theme.spacing(1),
      textAlign: "center",
      "&:last-child": {
        paddingRight: theme.spacing(1),
      },
    },
    expandCollapseCell: {
      cursor: "pointer",
    },
    expandCollapseIcon: {
      color: theme.palette.text.secondary,
      fontSize: "1.5em",
      verticalAlign: "middle",
    },
    extraInfo: {
      fontFamily: "SFMono-Regular,Consolas,Liberation Mono,Menlo,monospace",
      whiteSpace: "pre",
    },
  }),
);

type ArrayType<T> = T extends Array<infer U> ? U : never;
type Node = ArrayType<NodeInfoResponse["clients"]>;

type NodeRowGroupProps = {
  node: Node;
  clusterWorkers: Array<NodeInfoResponseWorker>;
  raylet: RayletInfoResponse["nodes"][keyof RayletInfoResponse["nodes"]] | null;
  logCounts: {
    perWorker: { [pid: string]: number };
    total: number;
  };
  errorCounts: {
    perWorker: { [pid: string]: number };
    total: number;
  };
  setLogDialog: (hostname: string, pid: number | null) => void;
  setErrorDialog: (hostname: string, pid: number | null) => void;
  initialExpanded: boolean;
};

const NodeRowGroup: React.FC<NodeRowGroupProps> = ({
  node,
  raylet,
  clusterWorkers,
  logCounts,
  errorCounts,
  setLogDialog,
  setErrorDialog,
  initialExpanded,
}) => {
  const [expanded, setExpanded] = useState<boolean>(initialExpanded);
  const toggleExpand = () => setExpanded(!expanded);
  const classes = useNodeRowGroupStyles();
  const features = [
    { NodeFeature: NodeHost, WorkerFeature: WorkerHost },
    {
      NodeFeature: NodeWorkers(clusterWorkers.length),
      WorkerFeature: WorkerWorkers,
    },
    { NodeFeature: NodeUptime, WorkerFeature: WorkerUptime },
    { NodeFeature: NodeCPU, WorkerFeature: WorkerCPU },
    { NodeFeature: NodeRAM, WorkerFeature: WorkerRAM },
    { NodeFeature: NodeGPU, WorkerFeature: WorkerGPU },
    { NodeFeature: NodeGRAM, WorkerFeature: WorkerGRAM },
    { NodeFeature: NodeDisk, WorkerFeature: WorkerDisk },
    { NodeFeature: NodeSent, WorkerFeature: WorkerSent },
    { NodeFeature: NodeReceived, WorkerFeature: WorkerReceived },
    {
      NodeFeature: makeNodeLogs(logCounts, setLogDialog),
      WorkerFeature: makeWorkerLogs(logCounts, setLogDialog),
    },
    {
      NodeFeature: makeNodeErrors(errorCounts, setErrorDialog),
      WorkerFeature: makeWorkerErrors(errorCounts, setErrorDialog),
    },
  ];

  return (
    <React.Fragment>
      <TableRow hover>
        <TableCell
          className={classNames(classes.cell, classes.expandCollapseCell)}
          onClick={toggleExpand}
        >
          {!expanded ? (
            <AddIcon className={classes.expandCollapseIcon} />
          ) : (
            <RemoveIcon className={classes.expandCollapseIcon} />
          )}
        </TableCell>
        {features.map(({ NodeFeature }, index) => (
          <TableCell className={classes.cell} key={index}>
            <NodeFeature node={node} />
          </TableCell>
        ))}
      </TableRow>
      {expanded && (
        <React.Fragment>
          {raylet !== null && raylet.extraInfo !== undefined && (
            <TableRow hover>
              <TableCell className={classes.cell} />
              <TableCell
                className={classNames(classes.cell, classes.extraInfo)}
                colSpan={features.length}
              >
                {raylet.extraInfo}
              </TableCell>
            </TableRow>
          )}
          {clusterWorkers.map((worker, index: number) => {
            const rayletWorker =
              raylet?.workersStats.find(
                (rayletWorker) => worker.pid === rayletWorker.pid,
              ) || null;

            return (
              <TableRow hover key={index}>
                <TableCell className={classes.cell} />
                {features.map(({ WorkerFeature }, index) => (
                  <TableCell className={classes.cell} key={index}>
                    <WorkerFeature
                      node={node}
                      worker={worker}
                      rayletWorker={rayletWorker}
                    />
                  </TableCell>
                ))}
              </TableRow>
            );
          })}
        </React.Fragment>
      )}
    </React.Fragment>
  );
};

export default NodeRowGroup;
