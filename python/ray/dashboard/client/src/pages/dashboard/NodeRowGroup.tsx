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
import CPU from "./features/CPU";
import Disk from "./features/Disk";
import { makeErrorsFeature } from "./features/Errors";
import Host from "./features/Host";
import { makeLogsFeature } from "./features/Logs";
import RAM from "./features/RAM";
import Uptime from "./features/Uptime";
import Workers from "./features/Workers";

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
      Host,
      Workers,
      Uptime,
      CPU,
      RAM,
      Disk,
      makeLogsFeature(logCounts),
      makeErrorsFeature(errorCounts)
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
          {features.map(Feature => (
            <TableCell className={classes.cell}>
              <Feature type="node" node={node} />
            </TableCell>
          ))}
        </TableRow>
        {expanded &&
          node.workers.map((worker, index: number) => (
            <TableRow hover key={index}>
              <TableCell className={classes.cell} />
              {features.map(Feature => (
                <TableCell className={classes.cell}>
                  <Feature type="worker" node={node} worker={worker} />
                </TableCell>
              ))}
            </TableRow>
          ))}
      </React.Fragment>
    );
  }
}

export default withStyles(styles)(NodeRowGroup);
