import {
  createStyles,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  Theme,
  withStyles,
  WithStyles,
  Button,
} from "@material-ui/core";
import React from "react";
import { StoreState } from "../../../store";
import { connect } from "react-redux";
import MemoryRowGroup from "./MemoryRowGroup";
import { dashboardActions } from "../state";

const styles = (theme: Theme) =>
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
  });

const mapStateToProps = (state: StoreState) => ({
  tab: state.dashboard.tab,
  memoryTable: state.dashboard.memoryTable,
});

const mapDispatchToProps = dashboardActions;

type State = {
  // If memory table is captured, it should stop reneweing memory table.
  memoryTableCapture: boolean;
};

class MemoryInfo extends React.Component<
  WithStyles<typeof styles> &
    ReturnType<typeof mapStateToProps> &
    typeof mapDispatchToProps,
  State
> {
  state: State = {
    memoryTableCapture: false,
  };

  handleMemoryTableCapture = () => {
    this.setState((state) => ({
      memoryTableCapture: !state.memoryTableCapture,
    }));
    this.props.setShouldObtainMemoryTable(this.state.memoryTableCapture);
  };

  render() {
    const { classes, memoryTable } = this.props;
    // console.log(memoryTable);
    return (
      <React.Fragment>
        {memoryTable !== null ? (
          <React.Fragment>
            <Button color="primary" onClick={this.handleMemoryTableCapture}>
              {this.state.memoryTableCapture
                ? "Stop capturing"
                : "Capture Memory Table"}
            </Button>
            <Table className={classes.table}>
              <TableHead>
                <TableRow>
                  <TableCell className={classes.cell} />
                  <TableCell className={classes.cell}>IP Adress</TableCell>
                  <TableCell className={classes.cell}>Pid</TableCell>
                  <TableCell className={classes.cell}>Type</TableCell>
                  <TableCell className={classes.cell}>Object ID</TableCell>
                  <TableCell className={classes.cell}>Object Size</TableCell>
                  <TableCell className={classes.cell}>Reference Type</TableCell>
                  <TableCell className={classes.cell}>Call Site</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {Object.keys(memoryTable.group).map((group_key, index) => (
                  <MemoryRowGroup
                    key={index}
                    groupKey={group_key}
                    memoryTableGroups={memoryTable.group}
                    initialExpanded={true}
                  />
                ))}
              </TableBody>
            </Table>
          </React.Fragment>
        ) : (
          <div>No Memory Table Information Provided</div>
        )}
      </React.Fragment>
    );
  }
}

export default connect(
  mapStateToProps,
  mapDispatchToProps,
)(withStyles(styles)(MemoryInfo));
