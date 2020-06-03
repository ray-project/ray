import {
  Button,
  createStyles,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  Theme,
  WithStyles,
  withStyles,
  FormControlLabel,
  Checkbox
} from "@material-ui/core";
import PauseIcon from "@material-ui/icons/Pause";
import PlayArrowIcon from "@material-ui/icons/PlayArrow";
import React, { ReactNode, useState } from "react";
import { connect } from "react-redux";
import { stopMemoryTableCollection } from "../../../api";
import { StoreState } from "../../../store";
import { dashboardActions } from "../state";
import MemoryRowGroup from "./MemoryRowGroup";
import { MemoryTableRow } from "./MemoryTableRow";

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

type Props = ReturnType<typeof mapStateToProps> &
  typeof mapDispatchToProps &
  WithStyles<typeof styles>;

const MemoryInfo = (props: Props) => {
  const handlePauseMemoryTable = async () => {
    props.setShouldObtainMemoryTable(!props.shouldObtainMemoryTable);
    if (props.shouldObtainMemoryTable) {
      await stopMemoryTableCollection();
    }
  };

  const pauseButtonIcon = props.shouldObtainMemoryTable ? (
    <PauseIcon />
  ) : (
    <PlayArrowIcon />
  );
  const [isGrouped, setIsGrouped] = useState(true);
  const { classes, memoryTable } = props;
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
          <Button color="primary" onClick={handlePauseMemoryTable}>
            {pauseButtonIcon}
            {props.shouldObtainMemoryTable
              ? "Pause Collection"
              : "Resume Collection"}
          </Button>
          <FormControlLabel
            control={
              <Checkbox
                checked={isGrouped}
                onChange={() => setIsGrouped(!isGrouped)}
                color="primary"
              />
            }
            label="Group by host"
          />
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
              {isGrouped
                ? Object.keys(memoryTable.group).map((group_key, index) => (
                    <MemoryRowGroup
                      key={index}
                      groupKey={group_key}
                      memoryTableGroups={memoryTable.group}
                      initialExpanded={true}
                    />
                  ))
                : Object.values(memoryTable.group).reduce(
                    (children: Array<ReactNode>, memoryTableGroup) => {
                      const groupChildren = memoryTableGroup.entries.map(
                        (memoryTableEntry, index) => (
                          <MemoryTableRow
                            memoryTableEntry={memoryTableEntry}
                            key={`mem-row-${index}`}
                          />
                        ),
                      );
                      return children.concat(groupChildren);
                    },
                    [],
                  )}
            </TableBody>
          </Table>
        </React.Fragment>
      ) : (
        <div>No Memory Table Information Provided</div>
      )}
    </React.Fragment>
  );
};

export default connect(
  mapStateToProps,
  mapDispatchToProps,
)(withStyles(styles)(MemoryInfo));
