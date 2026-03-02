import React from "react";
import {
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Box,
  Typography,
} from "@mui/material";
import { K8sEvent } from "../../type/k8s";
import { formatDateFromTimeMs } from "../../common/formatUtils";
import { StatusChip } from "../../components/StatusChip";

// We can reuse StatusChip or create a new one for Severity
const SeverityChip = ({ severity }: { severity: string }) => {
  let color = "default";
  if (severity === "WARNING") {
    color = "warning";
  } else if (severity === "ERROR") {
    color = "error";
  } else {
    color = "info"; // or success if appropriate
  }
  // StatusChip usually takes type and status.
  // We can just use a generic colored box or try to fit StatusChip.
  // StatusChip logic: ray/dashboard/client/src/components/StatusChip.tsx

  return (
    <StatusChip type="node" status={severity} />
  );
};

export const K8sEvents = ({ events }: { events: K8sEvent[] }) => {
  if (!events || events.length === 0) {
    return (
      <Box sx={{ padding: 2, textAlign: "center" }}>
        <Typography color="textSecondary">No Kubernetes events found for this node.</Typography>
      </Box>
    );
  }

  return (
    <TableContainer>
      <Table>
        <TableHead>
          <TableRow>
            <TableCell>Time</TableCell>
            <TableCell>Severity</TableCell>
            <TableCell>Reason</TableCell>
            <TableCell>Message</TableCell>
            <TableCell>Related ID</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {events.map((event, index) => (
            <TableRow key={event.eventId || index}>
              <TableCell>{formatDateFromTimeMs(event.timestampMs)}</TableCell>
              <TableCell>{event.severity}</TableCell>
              <TableCell>{event.reason}</TableCell>
              <TableCell>{event.message}</TableCell>
              <TableCell>{event.relatedNodeId}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableContainer>
  );
};
