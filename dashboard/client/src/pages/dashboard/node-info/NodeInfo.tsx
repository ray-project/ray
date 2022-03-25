import {
  Checkbox,
  createStyles,
  FormControlLabel,
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
import objectStoreMemoryFeature from "./features/ObjectStoreMemory";
import ramFeature from "./features/RAM";
import receivedFeature from "./features/Received";
import sentFeature from "./features/Sent";
import {
  Node,
  nodeInfoColumnId,
  NodeInfoFeature,
  WorkerFeatureData,
} from "./features/types";
import uptimeFeature from "./features/Uptime";
import workersFeature from "./features/Workers";
import NodeRowGroup from "./NodeRowGroup";
import { NodeWorkerRow } from "./NodeWorkerRow";
import TotalRow from "./TotalRow";

const sortWorkers = (
  workerFeatureData: WorkerFeatureData[],
  sortWorkerComparator: any,
) => {
  // Sorts idle workers to end, applies the worker comparator function to sort
  // then returns a new list of worker feature data.
  const idleSortedClusterWorkers = workerFeatureData.sort((wfd1, wfd2) => {
    const w1 = wfd1.worker;
    const w2 = wfd2.worker;
    if (w2.cmdline[0] === "ray::IDLE") {
      return -1;
    }
    if (w1.cmdline[0] === "ray::IDLE") {
      return 1;
    }
    return w1.pid < w2.pid ? -1 : 1;
  });
  return sortWorkerComparator
    ? stableSort(idleSortedClusterWorkers, sortWorkerComparator)
    : idleSortedClusterWorkers;
};

const makeGroupedTableContents = (
  nodes: Node[],
  sortWorkerComparator: any,
  sortGroupComparator: any,
  nodeInfoFeatures: NodeInfoFeature[],
) => {
  const sortedGroups = sortGroupComparator
    ? stableSort(nodes, sortGroupComparator)
    : nodes;
  return sortedGroups.map((node) => {
    const workerFeatureData: WorkerFeatureData[] = node.workers.map(
      (worker) => {
        return {
          node,
          worker,
        };
      },
    );

    const sortedClusterWorkers = sortWorkers(
      workerFeatureData,
      sortWorkerComparator,
    );
    return (
      <NodeRowGroup
        key={node.raylet.nodeId}
        node={node}
        workerFeatureData={sortedClusterWorkers}
        features={nodeInfoFeatures}
        initialExpanded={nodes.length <= 1}
      />
    );
  });
};

const makeUngroupedTableContents = (
  nodes: Node[],
  sortWorkerComparator: any,
  nodeInfoFeatures: NodeInfoFeature[],
) => {
  const workerInfoFeatures = nodeInfoFeatures.map(
    (feature) => feature.WorkerFeatureRenderFn,
  );
  const allWorkerFeatures: WorkerFeatureData[] = nodes.flatMap((node) => {
    return node.workers.map((worker) => {
      return {
        node,
        worker,
      };
    });
  });
  const sortedWorkers = sortWorkers(allWorkerFeatures, sortWorkerComparator);
  return sortedWorkers.map((workerFeatureDatum, i) => (
    <NodeWorkerRow
      features={workerInfoFeatures}
      data={workerFeatureDatum}
      key={`worker-${i}`}
    />
  ));
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

// Dead node payloads don't contain the full information needed to render
// so we filter them out here.
const liveNodesSelector = (state: StoreState) =>
  state.dashboard?.nodeInfo?.clients.filter(
    (node) => node.raylet.state === "ALIVE",
  );

type DialogState = {
  nodeIp: string;
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
  { id: "objectStoreMemory", label: "Plasma", numeric: false, sortable: true },
  { id: "disk", label: "Disk", numeric: true, sortable: true },
  { id: "sent", label: "Sent", numeric: true, sortable: true },
  { id: "received", label: "Received", numeric: false, sortable: true },
  { id: "logs", label: "Logs", numeric: false, sortable: true },
  { id: "errors", label: "Errors", numeric: false, sortable: true },
];

const NodeInfo: React.FC<{}> = () => {
  const [logDialog, setLogDialog] = useState<DialogState>(null);
  const [errorDialog, setErrorDialog] = useState<DialogState>(null);
  const [isGrouped, setIsGrouped] = useState(true);
  const [order, setOrder] = React.useState<Order>("asc");
  const toggleOrder = () => setOrder(order === "asc" ? "desc" : "asc");
  const [orderBy, setOrderBy] = React.useState<nodeInfoColumnId | null>(null);
  const classes = useNodeInfoStyles();
  const nodes = useSelector(liveNodesSelector);
  if (!nodes) {
    return <Typography color="textSecondary">Loading...</Typography>;
  }
  const clusterTotalWorkers = sum(nodes.map((n) => n.workers.length));
  const nodeInfoFeatures: NodeInfoFeature[] = [
    hostFeature,
    workersFeature,
    uptimeFeature,
    cpuFeature,
    ramFeature,
    gpuFeature,
    gramFeature,
    objectStoreMemoryFeature,
    diskFeature,
    sentFeature,
    receivedFeature,
    makeLogsFeature((nodeIp, pid) => setLogDialog({ nodeIp, pid })),
    makeErrorsFeature((nodeIp, pid) => setErrorDialog({ nodeIp, pid })),
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

  // Show GPU features only if there is at least one GPU in cluster.
  const showGPUs =
    nodes
      .map((n) => n.gpus)
      .filter((gpus) => gpus !== undefined && gpus.length !== 0).length !== 0;

  // Don't show disk on Kubernetes. K8s node disk usage should be monitored
  // elsewhere.
  // If a Ray node is running in a K8s pod, it marks available disk as 1 byte.
  // (See ReporterAgent._get_disk_usage() in reporter_agent.py)
  // Check if there are any nodes with realistic disk total:
  const showDisk = nodes.filter((n) => n.disk["/"].total > 10).length !== 0;

  const filterPredicate = (
    feature: NodeInfoFeature | HeaderInfo<nodeInfoColumnId>,
  ) =>
    (showGPUs || (feature.id !== "gpu" && feature.id !== "gram")) &&
    (showDisk || feature.id !== "disk");
  const filteredFeatures = nodeInfoFeatures.filter(filterPredicate);
  const filteredHeaders = nodeInfoHeaders.filter(filterPredicate);

  const tableContents = isGrouped
    ? makeGroupedTableContents(
        nodes,
        sortWorkerComparator,
        sortNodeComparator,
        filteredFeatures,
      )
    : makeUngroupedTableContents(nodes, sortWorkerComparator, filteredFeatures);
  return (
    <React.Fragment>
      <FormControlLabel
        control={
          <Checkbox
            checked={isGrouped}
            onChange={() => setIsGrouped(!isGrouped)}
            color="primary"
          />
        }
        label="Group by host"
      />
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
          headerInfo={filteredHeaders}
          order={order}
          orderBy={orderBy}
          firstColumnEmpty={true}
        />
        <TableBody>
          {tableContents}
          <TotalRow
            clusterTotalWorkers={clusterTotalWorkers}
            nodes={nodes}
            features={filteredFeatures.map(
              (feature) => feature.ClusterFeatureRenderFn,
            )}
          />
        </TableBody>
      </Table>
      {logDialog !== null && (
        <Logs
          clearLogDialog={() => setLogDialog(null)}
          nodeIp={logDialog.nodeIp}
          pid={logDialog.pid}
        />
      )}
      {errorDialog !== null && (
        <Errors
          clearErrorDialog={() => setErrorDialog(null)}
          nodeIp={errorDialog.nodeIp}
          pid={errorDialog.pid}
        />
      )}
    </React.Fragment>
  );
};

export default NodeInfo;
