import {
  Box,
  IconButton,
  TableCell,
  TableRow,
  Tooltip,
} from "@material-ui/core";
import AddIcon from "@material-ui/icons/Add";
import RemoveIcon from "@material-ui/icons/Remove";
import { sortBy } from "lodash";
import React, { useState } from "react";
import { Link } from "react-router-dom";
import useSWR from "swr";
import { API_REFRESH_INTERVAL_MS } from "../../common/constants";
import rowStyles from "../../common/RowStyles";
import PercentageBar from "../../components/PercentageBar";
import { StatusChip } from "../../components/StatusChip";
import { getNodeDetail } from "../../service/node";
import { NodeDetail } from "../../type/node";
import { Worker } from "../../type/worker";
import { memoryConverter } from "../../util/converter";
import { NodeGPUView, WorkerGPU } from "./GPUColumn";
import { NodeGRAM, WorkerGRAM } from "./GRAMColumn";

const TEXT_COL_MIN_WIDTH = 100;

type NodeRowProps = Pick<NodeRowsProps, "node"> & {
  /**
   * Whether the node has been expanded to show workers
   */
  expanded: boolean;
  /**
   * Click handler for when one clicks on the expand/unexpand button in this row.
   */
  onExpandButtonClick: () => void;
  newIA?: boolean;
};

/**
 * A single row that represents the node information only.
 * Does not show any data about the node's workers.
 */
export const NodeRow = ({
  node,
  expanded,
  onExpandButtonClick,
  newIA = false,
}: NodeRowProps) => {
  const {
    hostname = "",
    ip = "",
    cpu = 0,
    mem,
    disk,
    networkSpeed = [0, 0],
    raylet,
    logUrl,
  } = node;

  const classes = rowStyles();

  const objectStoreTotalMemory =
    raylet.objectStoreAvailableMemory + raylet.objectStoreUsedMemory;

  return (
    <TableRow>
      <TableCell>
        <IconButton size="small" onClick={onExpandButtonClick}>
          {!expanded ? (
            <AddIcon className={classes.expandCollapseIcon} />
          ) : (
            <RemoveIcon className={classes.expandCollapseIcon} />
          )}
        </IconButton>
      </TableCell>
      <TableCell align="center">
        <Box minWidth={TEXT_COL_MIN_WIDTH}>{hostname}</Box>
      </TableCell>
      <TableCell>
        <StatusChip type="node" status={raylet.state} />
      </TableCell>
      <TableCell align="center">
        <Tooltip title={raylet.nodeId} arrow interactive>
          <Link
            to={newIA ? `nodes/${raylet.nodeId}` : `/node/${raylet.nodeId}`}
            className={classes.idCol}
          >
            {raylet.nodeId}
          </Link>
        </Tooltip>
      </TableCell>
      <TableCell align="center">
        <Box minWidth={TEXT_COL_MIN_WIDTH}>
          {ip} {raylet.isHeadNode && "(Head)"}
        </Box>
      </TableCell>
      <TableCell>
        <Link
          to={
            newIA
              ? `/new/logs/${encodeURIComponent(logUrl)}`
              : `/log/${encodeURIComponent(logUrl)}`
          }
        >
          Log
        </Link>
      </TableCell>
      <TableCell>
        <PercentageBar num={Number(cpu)} total={100}>
          {cpu}%
        </PercentageBar>
      </TableCell>
      <TableCell>
        {mem && (
          <PercentageBar num={Number(mem[0] - mem[1])} total={mem[0]}>
            {memoryConverter(mem[0] - mem[1])}/{memoryConverter(mem[0])}(
            {mem[2].toFixed(1)}
            %)
          </PercentageBar>
        )}
      </TableCell>
      <TableCell>
        <NodeGPUView node={node} />
      </TableCell>
      <TableCell>
        <NodeGRAM node={node} />
      </TableCell>
      <TableCell>
        {raylet && objectStoreTotalMemory && (
          <PercentageBar
            num={raylet.objectStoreUsedMemory}
            total={objectStoreTotalMemory}
          >
            {memoryConverter(raylet.objectStoreUsedMemory)}/
            {memoryConverter(objectStoreTotalMemory)}(
            {(
              (raylet.objectStoreUsedMemory / objectStoreTotalMemory) *
              100
            ).toFixed(1)}
            %)
          </PercentageBar>
        )}
      </TableCell>
      <TableCell>
        {disk && disk["/"] && (
          <PercentageBar num={Number(disk["/"].used)} total={disk["/"].total}>
            {memoryConverter(disk["/"].used)}/{memoryConverter(disk["/"].total)}
            ({disk["/"].percent.toFixed(1)}%)
          </PercentageBar>
        )}
      </TableCell>
      <TableCell align="center">{memoryConverter(networkSpeed[0])}/s</TableCell>
      <TableCell align="center">{memoryConverter(networkSpeed[1])}/s</TableCell>
    </TableRow>
  );
};

type WorkerRowProps = {
  /**
   * Details of the worker
   */
  worker: Worker;
  /**
   * Detail of the node the worker is inside.
   */
  node: NodeDetail;
  newIA?: boolean;
};

/**
 * A single row that represents the data of a Worker
 */
export const WorkerRow = ({ node, worker, newIA = false }: WorkerRowProps) => {
  const classes = rowStyles();

  const { ip, mem, logUrl } = node;
  const {
    pid,
    cpuPercent: cpu = 0,
    memoryInfo,
    coreWorkerStats,
    cmdline,
  } = worker;

  const coreWorker = coreWorkerStats.length ? coreWorkerStats[0] : undefined;
  const workerLogUrl =
    (newIA
      ? `/new/logs/${encodeURIComponent(logUrl)}`
      : `/log/${encodeURIComponent(logUrl)}`) +
    (coreWorker ? `?fileName=${coreWorker.workerId}` : "");

  return (
    <TableRow>
      <TableCell>
        {/* Empty because workers do not have an expand / unexpand button. */}
      </TableCell>
      <TableCell align="center">{cmdline[0]}</TableCell>
      <TableCell>
        <StatusChip type="worker" status="ALIVE" />
      </TableCell>
      <TableCell align="center">
        {coreWorker && (
          <Tooltip title={coreWorker.workerId} arrow interactive>
            <span className={classes.idCol}>{coreWorker.workerId}</span>
          </Tooltip>
        )}
      </TableCell>
      <TableCell align="center">{pid}</TableCell>
      <TableCell>
        <Link to={workerLogUrl} target="_blank">
          Logs
        </Link>
        <br />
        <a
          href={`/worker/traceback?pid=${pid}&ip=${ip}&native=0`}
          target="_blank"
          title="Sample the current Python stack trace for this worker."
          rel="noreferrer"
        >
          Stack&nbsp;Trace
        </a>
        <br />
        <a
          href={`/worker/cpu_profile?pid=${pid}&ip=${ip}&duration=5&native=0`}
          target="_blank"
          title="Profile the Python worker for 5 seconds (default) and display a flame graph."
          rel="noreferrer"
        >
          Flame&nbsp;Graph
        </a>
        <br />
      </TableCell>
      <TableCell>
        <PercentageBar num={Number(cpu)} total={100}>
          {cpu}%
        </PercentageBar>
      </TableCell>
      <TableCell>
        {mem && (
          <PercentageBar num={memoryInfo.rss} total={mem[0]}>
            {memoryConverter(memoryInfo.rss)}/{memoryConverter(mem[0])}(
            {((memoryInfo.rss / mem[0]) * 100).toFixed(1)}
            %)
          </PercentageBar>
        )}
      </TableCell>
      <TableCell>
        <WorkerGPU worker={worker} />
      </TableCell>
      <TableCell>
        <WorkerGRAM worker={worker} node={node} />
      </TableCell>
      <TableCell>N/A</TableCell>
      <TableCell>N/A</TableCell>
      <TableCell align="center">N/A</TableCell>
      <TableCell align="center">N/A</TableCell>
    </TableRow>
  );
};

type NodeRowsProps = {
  /**
   * Details of the node
   */
  node: NodeDetail;
  /**
   * Whether the node row should refresh data about its workers.
   */
  isRefreshing: boolean;
  /**
   * Whether the row should start expanded. By default, this is false.
   */
  startExpanded?: boolean;
  newIA?: boolean;
};

/**
 * The rows related to a node and its workers. Expandable to show information about workers.
 */
export const NodeRows = ({
  node,
  isRefreshing,
  startExpanded = false,
  newIA = false,
}: NodeRowsProps) => {
  const [isExpanded, setExpanded] = useState(startExpanded);

  const { data } = useSWR(
    ["getNodeDetail", node.raylet.nodeId],
    async (_, nodeId) => {
      const { data } = await getNodeDetail(nodeId);
      const { data: rspData, result } = data;

      if (result === false) {
        console.error("Node Query Error Please Check Node Name");
      }

      if (rspData?.detail) {
        const sortedWorkers = sortBy(
          rspData.detail.workers,
          (worker) => worker.pid,
        );
        return sortedWorkers;
      }
    },
    { refreshInterval: isRefreshing ? API_REFRESH_INTERVAL_MS : 0 },
  );

  const workers = data ?? [];

  const handleExpandButtonClick = () => {
    setExpanded(!isExpanded);
  };

  return (
    <React.Fragment>
      <NodeRow
        node={node}
        expanded={isExpanded}
        onExpandButtonClick={handleExpandButtonClick}
        newIA={newIA}
      />
      {isExpanded &&
        workers.map((worker) => (
          <WorkerRow
            key={worker.pid}
            node={node}
            worker={worker}
            newIA={newIA}
          />
        ))}
    </React.Fragment>
  );
};
