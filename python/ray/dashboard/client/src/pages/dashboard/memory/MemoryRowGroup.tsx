import { createStyles, makeStyles, TableRow, Theme } from "@material-ui/core";
import AddIcon from "@material-ui/icons/Add";
import RemoveIcon from "@material-ui/icons/Remove";
import React, { useState } from "react";
import { MemoryTableEntry, MemoryTableSummary } from "../../../api";
import {
  ExpandableStyledTableCell,
  StyledTableCell,
} from "../../../common/TableCell";
import MemorySummary from "./MemorySummary";
import { MemoryTableRow } from "./MemoryTableRow";

const useMemoryRowGroupStyles = makeStyles((theme: Theme) =>
  createStyles({
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

type MemoryRowGroupProps = {
  groupKey: string;
  summary: MemoryTableSummary;
  entries: MemoryTableEntry[];
  initialExpanded: boolean;
};

const MemoryRowGroup: React.FC<MemoryRowGroupProps> = ({
  groupKey,
  entries,
  summary,
  initialExpanded,
}) => {
  const classes = useMemoryRowGroupStyles();
  const [expanded, setExpanded] = useState(initialExpanded);
  const toggleExpanded = () => setExpanded(!expanded);

  const features = [
    "node_ip_address",
    "pid",
    "type",
    "object_ref",
    "object_size",
    "reference_type",
    "call_site",
  ];

  return (
    <React.Fragment>
      <TableRow hover>
        <ExpandableStyledTableCell onClick={toggleExpanded}>
          {!expanded ? (
            <AddIcon className={classes.expandCollapseIcon} />
          ) : (
            <RemoveIcon className={classes.expandCollapseIcon} />
          )}
        </ExpandableStyledTableCell>
        {features.map((feature, index) => (
          <StyledTableCell key={index}>
            {// TODO(sang): For now, it is always grouped by node_ip_address.
            feature === "node_ip_address" ? groupKey : ""}
          </StyledTableCell>
        ))}
      </TableRow>
      {expanded && (
        <React.Fragment>
          <MemorySummary initialExpanded={false} memoryTableSummary={summary} />
          {entries.map((memoryTableEntry, index) => {
            return (
              <MemoryTableRow
                memoryTableEntry={memoryTableEntry}
                key={`${index}`}
              />
            );
          })}
        </React.Fragment>
      )}
    </React.Fragment>
  );
};

export default MemoryRowGroup;
