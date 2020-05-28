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
import PauseIcon from "@material-ui/icons/Pause";
import PlayArrowIcon from "@material-ui/icons/PlayArrow";
import React from "react";
import { StoreState } from "../../../store";
import { connect } from "react-redux";
import MemoryRowGroup from "./MemoryRowGroup";
import { dashboardActions } from "../state";
import { stopMemoryTableCollection } from "../../../api";

const styles = (theme: Theme) =>
  createStyles({
    table: {
      marginTop: theme.spacing(1),
    },
    cell: {
      padding: theme.spacing(1),
      textAlign: "center",
    },
  });

const mapStateToProps = (state: StoreState) => ({
  tab: state.dashboard.tab,
  memoryTable: state.dashboard.memoryTable,
  shouldObtainMemoryTable: state.dashboard.shouldObtainMemoryTable,
});

const mapDispatchToProps = dashboardActions;

type State = {
  // If memory table is captured, it should stop renewing memory table.
  pauseMemoryTable: boolean;
};

class MemoryInfo extends React.Component<
  WithStyles<typeof styles> &
    ReturnType<typeof mapStateToProps> &
    typeof mapDispatchToProps,
  State
> {
  handlePauseMemoryTable = async () => {
    const { shouldObtainMemoryTable } = this.props;
    this.props.setShouldObtainMemoryTable(!shouldObtainMemoryTable);
    if (shouldObtainMemoryTable) {
      await stopMemoryTableCollection();
    }
  };

  renderIcon = () => {
    if (this.props.shouldObtainMemoryTable) {
      return <PauseIcon />;
    } else {
      return <PlayArrowIcon />;
    }
  };

  render() {
    const { classes, memoryTable } = this.props;
    const memoryTableHeaders = [
      "", // Padding
      "IP Address",
      "Pid",
      "Type",
      "Object ID",
      "Object Size",
      "Reference Type",
      "Call Site",
    ];
    return (
      <React.Fragment>
        {memoryTable !== null ? (
          <React.Fragment>
            <Button color="primary" onClick={this.handlePauseMemoryTable}>
              {this.renderIcon()}
              {this.props.shouldObtainMemoryTable
                ? "Pause Collection"
                : "Resume Collection"}
            </Button>
            <Table className={classes.table}>
              <TableHead>
                <TableRow>
                  {memoryTableHeaders.map((header, index) => (
                    <TableCell key={index} className={classes.cell}>
                      {header}
                    </TableCell>
                  ))}
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
