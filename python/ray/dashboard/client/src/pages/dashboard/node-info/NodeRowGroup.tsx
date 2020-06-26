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
} from "../../../api";
import { StyledTableCell } from "../../../common/TableCell";
import { NodeInfoFeature, WorkerFeatureData } from "./features/types";
import { NodeWorkerRow } from "./NodeWorkerRow";

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
  features: NodeInfoFeature[];
  workerFeatureData: WorkerFeatureData[];
  initialExpanded: boolean;
};

const NodeRowGroup: React.FC<NodeRowGroupProps> = ({
  node,
  raylet,
  features,
  clusterWorkers,
  initialExpanded,
}) => {
  const [expanded, setExpanded] = useState<boolean>(initialExpanded);
  const toggleExpand = () => setExpanded(!expanded);
  const classes = useNodeRowGroupStyles();
  const renderedNodeFeatures = features.map((nodeInfoFeature, i) => {
    const FeatureComponent = nodeInfoFeature.NodeFeatureRenderFn;
    return (
      <StyledTableCell className={classes.cell} key={i}>
        <FeatureComponent node={node} />
      </StyledTableCell>
    );
  });
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
        {renderedNodeFeatures}
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
            const featureData = { rayletWorker, node, worker };
            return (
              <NodeWorkerRow
                key={index}
                features={features.map(
                  (feature) => feature.WorkerFeatureRenderFn,
                )}
                data={featureData}
              />
            );
          })}
        </React.Fragment>
      )}
    </React.Fragment>
  );
};

export default NodeRowGroup;
