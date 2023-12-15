import {
  Box,
  createStyles,
  IconButton,
  makeStyles,
  TableCell,
  TableRow,
  Tooltip,
} from "@material-ui/core";
import { sortBy } from "lodash";
import React, { useState } from "react";
import { RiArrowDownSLine, RiArrowRightSLine } from "react-icons/ri";
import { Link } from "react-router-dom";
import useSWR from "swr";
import { CodeDialogButtonWithPreview } from "../../common/CodeDialogButton";
import { API_REFRESH_INTERVAL_MS } from "../../common/constants";
import { NodeLink } from "../../common/links";
import {
  CpuProfilingLink,
  CpuStackTraceLink,
} from "../../common/ProfilingLink";
import rowStyles from "../../common/RowStyles";
import PercentageBar from "../../components/PercentageBar";
import { StatusChip } from "../../components/StatusChip";
import { getNodeDetail } from "../../service/node";
import { NodeDetail } from "../../type/node";
import { Worker } from "../../type/worker";
import { memoryConverter } from "../../util/converter";
import { NodeGPUView, WorkerGpuRow } from "./GPUColumn";
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
};

const useStyles = makeStyles((theme) =>
  createStyles({
    tableContainer: {
      overflowX: "scroll",
    },
    expandCollapseIcon: {
      color: theme.palette.text.secondary,
      fontSize: "1.5em",
      verticalAlign: "middle",
    },
    idCol: {
      display: "block",
      width: "50px",
      overflow: "hidden",
      textOverflow: "ellipsis",
      whiteSpace: "nowrap",
    },
    OverflowCol: {
      display: "block",
      width: "100px",
      overflow: "hidden",
      textOverflow: "ellipsis",
      whiteSpace: "nowrap",
    },
    helpInfo: {
      marginLeft: theme.spacing(1),
    },
    logicalResources: {
      maxWidth: 200,
    },
    labels: {
      maxWidth: 200,
    },
  }),
);

/**
 * A single row that represents the node information only.
 * Does not show any data about the node's workers.
 */
export const NodeRow = ({
  node,
  expanded,
  onExpandButtonClick,
}: NodeRowProps) => {
  const {
    hostname = "",
    ip = "",
    cpu = 0,
    mem,
    disk,
    networkSpeed = [0, 0],
    raylet,
    logicalResources,
  } = node;

  const classes = useStyles();

  const objectStoreTotalMemory =
    raylet.objectStoreAvailableMemory + raylet.objectStoreUsedMemory;

  /**
   * Why do we use raylet.state instead of node.state in the following code?
   * Because in ray, raylet == node
   */

  return (
    <TableRow>
      <TableCell>
        <IconButton size="small" onClick={onExpandButtonClick}>
          {!expanded ? (
            <RiArrowRightSLine className={classes.expandCollapseIcon} />
          ) : (
            <RiArrowDownSLine className={classes.expandCollapseIcon} />
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
          <div>
            <NodeLink
              nodeId={raylet.nodeId}
              to={`nodes/${raylet.nodeId}`}
              className={classes.idCol}
            />
          </div>
        </Tooltip>
      </TableCell>
      <TableCell align="center">
        <Box minWidth={TEXT_COL_MIN_WIDTH}>
          {ip} {raylet.isHeadNode && "(Head)"}
        </Box>
      </TableCell>
      <TableCell>
        {raylet.state !== "DEAD" && (
          <Link
            to={`/logs/?nodeId=${encodeURIComponent(raylet.nodeId)}`}
            style={{ textDecoration: "none" }}
          >
            Log
          </Link>
        )}
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
      <TableCell align="center">
        {logicalResources ? (
          <CodeDialogButtonWithPreview
            className={classes.logicalResources}
            title="Logical Resources"
            code={logicalResources}
          />
        ) : (
          "-"
        )}
      </TableCell>
      <TableCell align="center">
        <CodeDialogButtonWithPreview
          className={classes.labels}
          title="Labels"
          code={raylet.labels}
        />
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
export const WorkerRow = ({ node, worker }: WorkerRowProps) => {
  const classes = rowStyles();

  const {
    ip,
    mem,
    raylet: { nodeId },
  } = node;
  const {
    pid,
    cpuPercent: cpu = 0,
    memoryInfo,
    coreWorkerStats,
    cmdline,
  } = worker;

  const coreWorker = coreWorkerStats.length ? coreWorkerStats[0] : undefined;
  const workerLogUrl =
    `/logs/?nodeId=${encodeURIComponent(nodeId)}` +
    (coreWorker ? `&fileName=${coreWorker.workerId}` : "");

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
        <Link
          to={workerLogUrl}
          target="_blank"
          style={{ textDecoration: "none" }}
        >
          Log
        </Link>
        <br />
        <CpuProfilingLink pid={pid} ip={ip} type="" />
        <br />
        <CpuStackTraceLink pid={pid} ip={ip} type="" />
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
        <WorkerGpuRow worker={worker} node={node} />
      </TableCell>
      <TableCell>
        <WorkerGRAM worker={worker} node={node} />
      </TableCell>
      <TableCell>N/A</TableCell>
      <TableCell>N/A</TableCell>
      <TableCell align="center">N/A</TableCell>
      <TableCell align="center">N/A</TableCell>
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
};

/**
 * The rows related to a node and its workers. Expandable to show information about workers.
 */
export const NodeRows = ({
  node,
  isRefreshing,
  startExpanded = false,
}: NodeRowsProps) => {
  const [isExpanded, setExpanded] = useState(startExpanded);

  const { data } = useSWR(
    ["getNodeDetail", node.raylet.nodeId],
    async ([_, nodeId]) => {
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
      />
      {isExpanded &&
        workers.map((worker) => (
          <WorkerRow key={worker.pid} node={node} worker={worker} />
        ))}
    </React.Fragment>
  );
};
