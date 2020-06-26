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
import { NodeInfoFeature } from "../../../../../../../../bazel-ray/python/ray/dashboard/client/src/pages/dashboard/node-info/features/types";
import SortableTableHead, {
  HeaderInfo,
} from "../../../common/SortableTableHead";
import { getFnComparator, Order, stableSort } from "../../../common/tableUtils";
import { sum } from "../../../common/util";
import { StoreState } from "../../../store";
import Errors from "./dialogs/errors/Errors";
import Logs from "./dialogs/logs/Logs";
import cpuFeature from "./features/CPU";
import diskFeature from "./features/Disk";
import makeErrorsFeature from "./features/Errors";
import gpuFeature from "./features/GPU";
import gramFeature from "./features/GRAM";
import hostFeature from "./features/Host";
import makeLogsFeature from "./features/Logs";
import ramFeature from "./features/RAM";
import receivedFeature from "./features/Received";
import sentFeature from "./features/Sent";
import { nodeInfoColumnId } from "./features/types";
import uptimeFeature from "./features/Uptime";
import workersFeature from "./features/Workers";

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
  const clusterTotalWorkers = sum(
    nodeInfo.clients.map((c) => c.workers.length),
  );
  const nodeInfoFeatures: NodeInfoFeature[] = [
    hostFeature,
    workersFeature,
    uptimeFeature,
    cpuFeature,
    ramFeature,
    gpuFeature,
    gramFeature,
    diskFeature,
    sentFeature,
    receivedFeature,
    makeLogsFeature((hostname, pid) => setLogDialog({ hostname, pid })),
    makeErrorsFeature((hostname, pid) => setErrorDialog({ hostname, pid })),
  ];
  const sortNodeAccessor = nodeInfoFeatures.find(
    (feature) => feature.id === orderBy,
  )?.nodeAccessor;
  const sortNodeComparator =
    sortNodeAccessor && getFnComparator(order, sortNodeAccessor);
  const sortWorkerAccessor = nodeInfoFeatures.find(
    (feature) => feature.id === orderBy,
  )?.workerAccessor;
  const sortWorkerComparator =
    sortWorkerAccessor && getFnComparator(order, sortWorkerAccessor);
  return (
    <React.Fragment>
      <Table className={classes.table}>
        <SortableTableHead
          onRequestSort={(_, property) => {
            if (property === orderBy) {
              toggleOrder();
            } else {
              setOrderBy(property);
              setOrder("asc");
            }
          }}
          headerInfo={nodeInfoHeaders}
          order={order}
          orderBy={orderBy}
          firstColumnEmpty={true}
        />
        <TableBody>
          {nodeInfo.clients.map((client) => {
            const idleSortedClusterWorkers = [...client.workers].sort(
              (w1, w2) => {
                if (w2.cmdline[0] === "ray::IDLE") {
                  return -1;
                }
                if (w1.cmdline[0] === "ray::IDLE") {
                  return 1;
                }
                return w1.pid < w2.pid ? -1 : 1;
              },
            );
            const sortedClusterWorkers = sortWorkerComparator
              ? stableSort(idleSortedClusterWorkers, sortWorkerComparator)
              : idleSortedClusterWorkers;
            return (
              <NodeRowGroup
                key={client.ip}
                clusterWorkers={}
                node={client}
                raylet={
                  client.ip in rayletInfo.nodes
                    ? rayletInfo.nodes[client.ip]
                    : null
                }
                features={nodeInfoFeatures}
                initialExpanded={nodeInfo.clients.length <= 1}
              />
            );
          })}
          <TotalRow
            clusterTotalWorkers={clusterTotalWorkers}
            nodes={nodeInfo.clients}
            features={nodeInfoFeatures.map(
              (feature) => feature.ClusterFeatureRenderFn,
            )}
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
