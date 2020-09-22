import { TableRow } from "@material-ui/core";
import React from "react";
import { MemoryTableEntry } from "../../../api";
import { formatByteAmount } from "../../../common/formatUtils";
import { StyledTableCell } from "../../../common/TableCell";

type Props = {
  memoryTableEntry: MemoryTableEntry;
};

export const MemoryTableRow = (props: Props) => {
  const { memoryTableEntry } = props;
  const object_size =
    memoryTableEntry.object_size === -1
      ? "?"
      : formatByteAmount(memoryTableEntry.object_size, "mebibyte");
  const memoryTableEntryValues = [
    memoryTableEntry.node_ip_address,
    memoryTableEntry.pid,
    memoryTableEntry.type,
    memoryTableEntry.object_ref,
    object_size,
    memoryTableEntry.reference_type,
    memoryTableEntry.call_site,
  ];
  return (
    <TableRow hover>
      {memoryTableEntryValues.map((value, index) => (
        <StyledTableCell key={`${index}`}>{value}</StyledTableCell>
      ))}
    </TableRow>
  );
};
