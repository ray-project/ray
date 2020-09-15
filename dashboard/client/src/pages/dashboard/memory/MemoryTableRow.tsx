import { TableRow } from "@material-ui/core";
import React from "react";
import { MemoryTableEntry } from "../../../api";
import { formatByteAmount } from "../../../common/formatUtils";
import { StyledTableCell } from "../../../common/TableCell";

type Props = {
  memoryTableEntry: MemoryTableEntry;
  key: string;
};

export const MemoryTableRow = (props: Props) => {
  const { memoryTableEntry, key } = props;
  const object_size =
    memoryTableEntry.objectSize === -1
      ? "?"
      : formatByteAmount(memoryTableEntry.objectSize, "mebibyte");
  const memoryTableEntryValues = [
    memoryTableEntry.nodeIpAddress,
    memoryTableEntry.pid,
    memoryTableEntry.type,
    memoryTableEntry.objectRef,
    object_size,
    memoryTableEntry.referenceType,
    memoryTableEntry.callSite,
  ];
  return (
    <TableRow hover key={key}>
      {memoryTableEntryValues.map((value, index) => (
        <StyledTableCell key={`${key}-${index}`}>{value}</StyledTableCell>
      ))}
    </TableRow>
  );
};
