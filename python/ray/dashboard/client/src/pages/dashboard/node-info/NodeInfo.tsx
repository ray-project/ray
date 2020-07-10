import {
  createStyles,
  makeStyles,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  Theme,
  Typography,
} from "@material-ui/core";
import React, { useState } from "react";
import { useSelector } from "react-redux";
import { RayletInfoResponse } from "../../../api";
import { sum } from "../../../common/util";
import { StoreState } from "../../../store";
import Errors from "./dialogs/errors/Errors";
import Logs from "./dialogs/logs/Logs";
import NodeRowGroup from "./NodeRowGroup";
import TotalRow from "./TotalRow";

const clusterWorkerPids = (
  rayletInfo: RayletInfoResponse,
): Map<string, Set<string>> => {
  // Groups PIDs registered with the raylet by node IP address
  // This is used to filter out processes belonging to other ray clusters.
  const nodeMap = new Map();
  const workerPids = new Set();
  for (const [nodeIp, { workersStats }] of Object.entries(rayletInfo.nodes)) {
    for (const worker of workersStats) {
      if (!worker.isDriver) {
        workerPids.add(worker.pid.toString());
      }
    }
    nodeMap.set(nodeIp, workerPids);
  }
  return nodeMap;
};

const useNodeInfoStyles = makeStyles((theme: Theme) =>
  createStyles({
    table: {
      marginTop: theme.spacing(1),
    },
    cell: {
      padding: theme.spacing(1),
      textAlign: "center",
      "&:last-child": {
        paddingRight: theme.spacing(1),
      },
    },
  }),
);

const nodeInfoSelector = (state: StoreState) => ({
  nodeInfo: state.dashboard.nodeInfo,
  rayletInfo: state.dashboard.rayletInfo,
});

type dialogState = {
  hostname: string;
  pid: number | null;
} | null;

const NodeInfo: React.FC<{}> = () => {
  const [logDialog, setLogDialog] = useState<dialogState>(null);
  const [errorDialog, setErrorDialog] = useState<dialogState>(null);
  const classes = useNodeInfoStyles();
  const { nodeInfo, rayletInfo } = useSelector(nodeInfoSelector);

  if (nodeInfo === null || rayletInfo === null) {
    return <Typography color="textSecondary">Loading...</Typography>;
  }

  const logCounts: {
    [ip: string]: {
      perWorker: {
        [pid: string]: number;
      };
      total: number;
    };
  } = {};

  const errorCounts: {
    [ip: string]: {
      perWorker: {
        [pid: string]: number;
      };
      total: number;
    };
  } = {};

  // We fetch data about which process IDs are registered with
  // the cluster's raylet for each node. We use this to filter
  // the worker data contained in the node info data because
  // the node info can contain data from more than one cluster
  // if more than one cluster is running on a machine.
  const clusterWorkerPidsByIp = clusterWorkerPids(rayletInfo);
  const clusterTotalWorkers = sum(
    Array.from(clusterWorkerPidsByIp.values()).map(
      (workerSet) => workerSet.size,
    ),
  );
  // Initialize inner structure of the count objects
  for (const client of nodeInfo.clients) {
    const clusterWorkerPids = clusterWorkerPidsByIp.get(client.ip);
    if (!clusterWorkerPids) {
      continue;
    }
    const filteredLogEntries = Object.entries(
      nodeInfo.log_counts[client.ip] || {},
    ).filter(([pid, _]) => clusterWorkerPids.has(pid));
    const totalLogEntries = sum(filteredLogEntries.map(([_, count]) => count));
    logCounts[client.ip] = {
      perWorker: Object.fromEntries(filteredLogEntries),
      total: totalLogEntries,
    };

    const filteredErrEntries = Object.entries(
      nodeInfo.error_counts[client.ip] || {},
    ).filter(([pid, _]) => clusterWorkerPids.has(pid));
    const totalErrEntries = sum(filteredErrEntries.map(([_, count]) => count));
    errorCounts[client.ip] = {
      perWorker: Object.fromEntries(filteredErrEntries),
      total: totalErrEntries,
    };
  }

  return (
    <React.Fragment>
      <Table className={classes.table}>
        <TableHead>
          <TableRow>
            <TableCell className={classes.cell} />
            <TableCell className={classes.cell}>Host</TableCell>
            <TableCell className={classes.cell}>Workers</TableCell>
            <TableCell className={classes.cell}>Uptime</TableCell>
            <TableCell className={classes.cell}>CPU</TableCell>
            <TableCell className={classes.cell}>RAM</TableCell>
            <TableCell className={classes.cell}>GPU</TableCell>
            <TableCell className={classes.cell}>GRAM</TableCell>
            <TableCell className={classes.cell}>Disk</TableCell>
            <TableCell className={classes.cell}>Sent</TableCell>
            <TableCell className={classes.cell}>Received</TableCell>
            <TableCell className={classes.cell}>Logs</TableCell>
            <TableCell className={classes.cell}>Errors</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {nodeInfo.clients.map((client) => {
            const clusterWorkerPids =
              clusterWorkerPidsByIp.get(client.ip) || new Set();
            return (
              <NodeRowGroup
                key={client.ip}
                clusterWorkers={client.workers
                  .filter((worker) =>
                    clusterWorkerPids.has(worker.pid.toString()),
                  )
                  .sort((w1, w2) => {
                    if (w2.cmdline[0] === "ray::IDLE") {
                      return -1;
                    }
                    if (w1.cmdline[0] === "ray::IDLE") {
                      return 1;
                    }
                    return w1.pid < w2.pid ? -1 : 1;
                  })}
                node={client}
                raylet={
                  client.ip in rayletInfo.nodes
                    ? rayletInfo.nodes[client.ip]
                    : null
                }
                logCounts={logCounts[client.ip]}
                errorCounts={errorCounts[client.ip]}
                setLogDialog={(hostname, pid) =>
                  setLogDialog({ hostname, pid })
                }
                setErrorDialog={(hostname, pid) =>
                  setErrorDialog({ hostname, pid })
                }
                initialExpanded={nodeInfo.clients.length <= 1}
              />
            );
          })}
          <TotalRow
            clusterTotalWorkers={clusterTotalWorkers}
            nodes={nodeInfo.clients}
            logCounts={logCounts}
            errorCounts={errorCounts}
          />
        </TableBody>
      </Table>
      {logDialog !== null && (
        <Logs
          clearLogDialog={() => setLogDialog(null)}
          hostname={logDialog.hostname}
          pid={logDialog.pid}
        />
      )}
      {errorDialog !== null && (
        <Errors
          clearErrorDialog={() => setErrorDialog(null)}
          hostname={errorDialog.hostname}
          pid={errorDialog.pid}
        />
      )}
    </React.Fragment>
  );
};

export default NodeInfo;
