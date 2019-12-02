import { Theme } from "@material-ui/core/styles/createMuiTheme";
import createStyles from "@material-ui/core/styles/createStyles";
import withStyles, { WithStyles } from "@material-ui/core/styles/withStyles";
import TableCell from "@material-ui/core/TableCell";
import TableRow from "@material-ui/core/TableRow";
import AddIcon from "@material-ui/icons/Add";
import RemoveIcon from "@material-ui/icons/Remove";
import classNames from "classnames";
import React from "react";
import { NodeInfoResponse } from "../../api";
import { NodeCPU, WorkerCPU } from "./features/CPU";
import { NodeDisk, WorkerDisk } from "./features/Disk";
import { makeNodeErrors, makeWorkerErrors } from "./features/Errors";
import { NodeHost, WorkerHost } from "./features/Host";
import { makeNodeLogs, makeWorkerLogs } from "./features/Logs";
import { NodeRAM, WorkerRAM } from "./features/RAM";
import { NodeUptime, WorkerUptime } from "./features/Uptime";
import { NodeWorkers, WorkerWorkers } from "./features/Workers";

const styles = (theme: Theme) =>
  createStyles({
    cell: {
      padding: theme.spacing(1),
      textAlign: "center",
      "&:last-child": {
        paddingRight: theme.spacing(1)
      }
    },
    expandCollapseCell: {
      cursor: "pointer"
    },
    expandCollapseIcon: {
      color: theme.palette.text.secondary,
      fontSize: "1.5em",
      verticalAlign: "middle"
    }
  });

type ArrayType<T> = T extends Array<infer U> ? U : never;
type Node = ArrayType<NodeInfoResponse["clients"]>;

interface Props {
  node: Node;
  logCounts: {
    perWorker: { [pid: string]: number };
    total: number;
  };
  errorCounts: {
    perWorker: { [pid: string]: number };
    total: number;
  };
}

interface State {
  expanded: boolean;
}

class NodeRowGroup extends React.Component<
  Props & WithStyles<typeof styles>,
  State
> {
  state: State = {
    expanded: false
  };

  toggleExpand = () => {
    this.setState(state => ({
      expanded: !state.expanded
    }));
  };

  render() {
    const { classes, node, logCounts, errorCounts } = this.props;
    const { expanded } = this.state;

    const features = [
      { NodeFeature: NodeHost, WorkerFeature: WorkerHost },
      { NodeFeature: NodeWorkers, WorkerFeature: WorkerWorkers },
      { NodeFeature: NodeUptime, WorkerFeature: WorkerUptime },
      { NodeFeature: NodeCPU, WorkerFeature: WorkerCPU },
      { NodeFeature: NodeRAM, WorkerFeature: WorkerRAM },
      { NodeFeature: NodeDisk, WorkerFeature: WorkerDisk },
      {
        NodeFeature: makeNodeLogs(logCounts),
        WorkerFeature: makeWorkerLogs(logCounts)
      },
      {
        NodeFeature: makeNodeErrors(errorCounts),
        WorkerFeature: makeWorkerErrors(errorCounts)
      }
    ];

    return (
      <React.Fragment>
        <TableRow hover>
          <TableCell
            className={classNames(classes.cell, classes.expandCollapseCell)}
            onClick={this.toggleExpand}
          >
            {!expanded ? (
              <AddIcon className={classes.expandCollapseIcon} />
            ) : (
              <RemoveIcon className={classes.expandCollapseIcon} />
            )}
          </TableCell>
          {features.map(({ NodeFeature }) => (
            <TableCell className={classes.cell}>
              <NodeFeature node={node} />
            </TableCell>
          ))}
        </TableRow>
        {expanded &&
          node.workers.map((worker, index: number) => (
            <TableRow hover key={index}>
              <TableCell className={classes.cell} />
              {features.map(({ WorkerFeature }) => (
                <TableCell className={classes.cell}>
                  <WorkerFeature node={node} worker={worker} />
                </TableCell>
              ))}
            </TableRow>
          ))}
      </React.Fragment>
    );
  }
}

export default withStyles(styles)(NodeRowGroup);
