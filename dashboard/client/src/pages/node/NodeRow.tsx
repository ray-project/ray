import { IconButton, TableCell, TableRow, Tooltip } from "@material-ui/core";
import AddIcon from "@material-ui/icons/Add";
import RemoveIcon from "@material-ui/icons/Remove";
import { sortBy } from "lodash";
import React, { useCallback, useEffect, useRef, useState } from "react";
import { Link } from "react-router-dom";
import rowStyles from "../../common/RowStyles";
import PercentageBar from "../../components/PercentageBar";
import { StatusChip } from "../../components/StatusChip";
import { getNodeDetail } from "../../service/node";
import { NodeDetail } from "../../type/node";
import { Worker } from "../../type/worker";
import { memoryConverter } from "../../util/converter";

type NodeRowProps = Pick<NodeRowsProps, "node"> & {
  /**
   * Whether the node has been expanded to show workers
   */
  expanded: boolean;
  /**
   * Click handler for when one clicks on the expand/unexpand button in this row.
   */
  onExpandButtonClick: () => void;
};

/**
 * A single row that represents the node information only.
 * Does not show any data about the node's workers.
 */
const NodeRow = ({ node, expanded, onExpandButtonClick }: NodeRowProps) => {
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
      <TableCell>
        <StatusChip type="node" status={raylet.state} />
      </TableCell>
      <TableCell align="center">
        <Tooltip title={raylet.nodeId} arrow interactive>
          <Link to={`/node/${raylet.nodeId}`} className={classes.idCol}>
            {raylet.nodeId}
          </Link>
        </Tooltip>
      </TableCell>
      <TableCell align="center">{hostname}</TableCell>
      <TableCell align="center">{ip}</TableCell>
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
        {raylet && raylet.objectStoreUsedMemory && (
          <PercentageBar
            num={raylet.objectStoreUsedMemory}
            total={objectStoreTotalMemory}
          >
            {memoryConverter(raylet.objectStoreUsedMemory)}/
            {memoryConverter(objectStoreTotalMemory)}
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
      <TableCell>
        <Link to={`/log/${encodeURIComponent(logUrl)}`}>Log</Link>
      </TableCell>
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
};

/**
 * A single row that represents the data of a Worker
 */
const WorkerRow = ({ node, worker }: WorkerRowProps) => {
  const classes = rowStyles();

  const { mem, logUrl } = node;
  const {
    pid,
    cpuPercent: cpu = 0,
    memoryInfo,
    coreWorkerStats,
    cmdline,
  } = worker;

  const coreWorker = coreWorkerStats.length ? coreWorkerStats[0] : undefined;
  const workerLogUrl = coreWorker
    ? `/log/${encodeURIComponent(logUrl)}?fileName=${coreWorker.workerId}`
    : `/log/${encodeURIComponent(logUrl)}`;

  return (
    <TableRow>
      <TableCell>
        {/* Empty because workers do not have an expand / unexpand button. */}
      </TableCell>
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
      <TableCell align="center">{cmdline[0]}</TableCell>
      <TableCell align="center">{pid}</TableCell>
      <TableCell>
        <PercentageBar num={Number(cpu)} total={100}>
          {cpu}%
        </PercentageBar>
      </TableCell>
      <TableCell>
        {mem && (
          <PercentageBar num={memoryInfo.rss} total={mem[0]}>
            {memoryConverter(memoryInfo.rss)}/{memoryConverter(mem[0])}(
            {(memoryInfo.rss / mem[0]).toFixed(1)}
            %)
          </PercentageBar>
        )}
      </TableCell>
      <TableCell>N/A</TableCell>
      <TableCell>N/A</TableCell>
      <TableCell align="center">N/A</TableCell>
      <TableCell align="center">N/A</TableCell>
      <TableCell>
        <Link to={workerLogUrl} target="_blank">
          Log
        </Link>
      </TableCell>
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
};

/**
 * The rows related to a node and its workers. Expandable to show information about workers.
 */
export const NodeRows = ({ node, isRefreshing }: NodeRowsProps) => {
  const [isExpanded, setExpanded] = useState(false);
  const [workers, setWorkers] = useState<Worker[]>([]);
  const tot = useRef<NodeJS.Timeout>();

  const getDetail = useCallback(async () => {
    if (!isRefreshing || !isExpanded) {
      return;
    }
    const { data } = await getNodeDetail(node.raylet.nodeId);
    const { data: rspData, result } = data;
    if (rspData?.detail) {
      const sortedWorkers = sortBy(
        rspData.detail.workers,
        (worker) => worker.pid,
      );
      setWorkers(sortedWorkers);
    }

    if (result === false) {
      console.error("Node Query Error Please Check Node Name");
    }

    tot.current = setTimeout(getDetail, 4000);
  }, [isRefreshing, isExpanded, node.raylet.nodeId]);

  useEffect(() => {
    getDetail();
    return () => {
      if (tot.current) {
        clearTimeout(tot.current);
      }
    };
  }, [getDetail]);

  const handleExpandButtonClick = () => {
    setExpanded(!isExpanded);
  };

  return (
    <React.Fragment>
      <NodeRow
        node={node}
        expanded={isExpanded}
        onExpandButtonClick={handleExpandButtonClick}
      />
      {isExpanded &&
        workers.map((worker) => (
          <WorkerRow key={worker.pid} node={node} worker={worker} />
        ))}
    </React.Fragment>
  );
};
