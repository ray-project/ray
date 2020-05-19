import {
  createStyles,
  TableCell,
  TableRow,
  Theme,
  withStyles,
  WithStyles,
} from "@material-ui/core";
import React from "react";
import { MemoryTableSummary } from "../../../api";

const styles = (theme: Theme) =>
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
  });

type Props = {
  memoryTableSummary: MemoryTableSummary;
  initialExpanded: boolean;
};

type State = {
  expanded: boolean;
};

class MemorySummary extends React.Component<
  Props & WithStyles<typeof styles>,
  State
> {
  state: State = {
    expanded: this.props.initialExpanded,
  };

  toggleExpand = () => {
    this.setState((state) => ({
      expanded: !state.expanded,
    }));
  };

  render() {
    const { classes, memoryTableSummary } = this.props;

    return (
      memoryTableSummary !== null && (
        <React.Fragment>
          <TableRow hover>
            <TableCell className={classes.cell} />
            <TableCell className={classes.cell}>
              {"Total Local Reference Count: " +
                memoryTableSummary.total_local_ref_count}
            </TableCell>
            <TableCell className={classes.cell}>
              {"Total Pinned In Memory Count: " +
                memoryTableSummary.total_pinned_in_memory}
            </TableCell>
            <TableCell className={classes.cell}>
              {"Total Used By Pending Tasks Count: " +
                memoryTableSummary.total_used_by_pending_task}
            </TableCell>
            <TableCell className={classes.cell}>
              {"Total Caputed In Objects Count: " +
                memoryTableSummary.total_captured_in_objects}
            </TableCell>
            <TableCell className={classes.cell}>
              {"Total Object Size: " +
                memoryTableSummary.total_object_size +
                " B"}
            </TableCell>
            <TableCell className={classes.cell}>
              {"Total Actor Handle Count: " +
                memoryTableSummary.total_actor_handles}
            </TableCell>
            <TableCell className={classes.cell} />
          </TableRow>
        </React.Fragment>
      )
    );
  }
}

export default withStyles(styles)(MemorySummary);
