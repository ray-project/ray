import { IconButton, TableCell, TableRow, Tooltip } from "@material-ui/core";
import { createStyles, makeStyles } from "@material-ui/core/styles";
import AddIcon from "@material-ui/icons/Add";
import RemoveIcon from "@material-ui/icons/Remove";
import React, { useState } from "react";
import { Link } from "react-router-dom";
import PercentageBar from "../../components/PercentageBar";
import { StatusChip } from "../../components/StatusChip";
import { NodeDetailExtend } from "../../type/node";
import { Worker } from "../../type/worker";
import { memoryConverter } from "../../util/converter";

const useNodeRowStyles = makeStyles((theme) =>
  createStyles({
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
  }),
);

type NodeRowProps = {
  node: NodeDetailExtend;
  rowIndex: number;
  expanded: boolean;
  onExpandButtonClick: () => void;
};

const NodeRow = ({
  node,
  rowIndex,
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
    logUrl,
  } = node;

  const classes = useNodeRowStyles();

  return (
    <TableRow key={hostname + rowIndex}>
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
        <PercentageBar num={Number(mem[0] - mem[1])} total={mem[0]}>
          {memoryConverter(mem[0] - mem[1])}/{memoryConverter(mem[0])}(
          {mem[2].toFixed(1)}
          %)
        </PercentageBar>
      </TableCell>
      <TableCell>
        {raylet && (
          <PercentageBar
            num={raylet.objectStoreUsedMemory}
            total={raylet.objectStoreAvailableMemory}
          >
            {memoryConverter(raylet.objectStoreUsedMemory)}/
            {memoryConverter(raylet.objectStoreAvailableMemory)}
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
  node: NodeDetailExtend;
  worker: Worker;
};

const WorkerRow = ({ node, worker }: WorkerRowProps) => {
  const classes = useNodeRowStyles();

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
    <TableRow key={coreWorker?.workerId ?? pid}>
      <TableCell></TableCell>
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
        <PercentageBar num={memoryInfo.rss} total={mem[0]}>
          {memoryConverter(memoryInfo.rss)}/{memoryConverter(mem[0])}(
          {(memoryInfo.rss / mem[0]).toFixed(1)}
          %)
        </PercentageBar>
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
  node: NodeDetailExtend;
  rowIndex: number;
};

/**
 * The rows related to a node and its workers. Expandable to show information about workers.
 */
export const NodeRows = ({ node, rowIndex }: NodeRowsProps) => {
  const [isExpanded, setExpanded] = useState(false);

  const handleExpandButtonClick = () => {
    setExpanded(!isExpanded);
  };

  return (
    <React.Fragment>
      <NodeRow
        node={node}
        rowIndex={rowIndex}
        expanded={isExpanded}
        onExpandButtonClick={handleExpandButtonClick}
      />
      {isExpanded &&
        node.workers.map((worker) => <WorkerRow node={node} worker={worker} />)}
    </React.Fragment>
  );
};
