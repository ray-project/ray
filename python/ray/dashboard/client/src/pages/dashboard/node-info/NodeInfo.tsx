import {
  createStyles,
  makeStyles,
  Table,
  TableBody,
  Theme,
  Typography,
} from "@material-ui/core";
import React, { useState } from "react";
import { useSelector } from "react-redux";
import SortableTableHead, {
  HeaderInfo,
} from "../../../common/SortableTableHead";
import { getComparator, Order, stableSort } from "../../../common/tableUtils";
import { sum } from "../../../common/util";
import { StoreState } from "../../../store";
import Errors from "./dialogs/errors/Errors";
import Logs from "./dialogs/logs/Logs";
import NodeRowGroup from "./NodeRowGroup";
import TotalRow from "./TotalRow";

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

type DialogState = {
  hostname: string;
  pid: number | null;
} | null;

type NodeAggregation = {
    [ip: string]: {
      perWorker: {
        [pid: string]: number;
      };
      total: number;
    };
  }

type nodeInfoColumnId = "host" | "workers" | "uptime" | "cpu" | "ram" | "gpu" | "gram" | "disk" | "sent" | "received" | "logs" | "errors"

const nodeInfoHeaders: HeaderInfo<nodeInfoColumnId>[] = [
  { id: "host", label: "Host", numeric: true, sortable: true },
  { id: "workers", label: "PID", numeric: true, sortable: false },
  { id: "uptime", label: "Uptime (s)", numeric: true, sortable: true },
  { id: "cpu", label: "CPU", numeric: false, sortable: true },
  { id: "ram", label: "RAM", numeric: true, sortable: true },
  { id: "gpu", label: "GPU", numeric: true, sortable: true },
  { id: "gram", label: "GRAM", numeric: true, sortable: true },
  { id: "disk", label: "Disk", numeric: true, sortable: true },
  { id: "sent", label: "Sent", numeric: true, sortable: true },
  { id: "received", label: "Received", numeric: false, sortable: true },
  { id: "logs", label: "Logs", numeric: false, sortable: false },
  { id: "errors", label: "Errors", numeric: false, sortable: false },
];

const NodeInfo: React.FC<{}> = () => {
  const [logDialog, setLogDialog] = useState<DialogState>(null);
  const [errorDialog, setErrorDialog] = useState<DialogState>(null);
  const [isGrouped, setIsGrouped] = useState(true);
  const [order, setOrder] = React.useState<Order>("asc");
  const toggleOrder = () => setOrder(order === "asc" ? "desc" : "asc");

  const [orderBy, setOrderBy] = React.useState<string | null>(null);
  const classes = useNodeInfoStyles();
  const { nodeInfo, rayletInfo } = useSelector(nodeInfoSelector);

  if (nodeInfo === null || rayletInfo === null) {
    return <Typography color="textSecondary">Loading...</Typography>;
  }
  const clusterTotalWorkers = sum(nodeInfo.clients.map(c => c.workers.length));

  const logCounts: NodeAggregation = {};
  const errorCounts: NodeAggregation = {};
  // Initialize inner structure of the count objects
  for (const client of nodeInfo.clients) {
    const nodeLogCounts = nodeInfo.log_counts[client.ip] || {};
    const totalLogEntries = sum(Object.values(nodeLogCounts));
    logCounts[client.ip] = {
      perWorker: nodeLogCounts,
      total: totalLogEntries,
    };


    const nodeErrCounts = nodeInfo.error_counts[client.ip] || {};
    const totalErrEntries = sum(Object.values(nodeErrCounts));
    errorCounts[client.ip] = {
      perWorker: nodeErrCounts,
      total: totalErrEntries,
    };
  }

  return (
    <React.Fragment>
      <Table className={classes.table}>
        <SortableTableHead
          onRequestSort={() => {}}
          headerInfo={nodeInfoHeaders}
          order={order}
          orderBy={orderBy}
        />
        <TableBody>
          {nodeInfo.clients.map((client) => {
            return (
              <NodeRowGroup
                key={client.ip}
                clusterWorkers={[...client.workers]
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
