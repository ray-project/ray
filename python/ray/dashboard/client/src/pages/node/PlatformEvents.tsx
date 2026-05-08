import {
  Box,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from "@mui/material";
import React from "react";
import { formatDateFromTimeMs } from "../../common/formatUtils";
import { StatusChip } from "../../components/StatusChip";
import { PlatformEvent } from "../../type/platform";

const SeverityChip = ({ severity }: { severity: string }) => {
  return <StatusChip type="severity" status={severity} />;
};

export const PlatformEvents = ({
  events,
  emptyMessage = "No platform events found.",
  isLoading = false,
}: {
  events: PlatformEvent[];
  emptyMessage?: string;
  isLoading?: boolean;
}) => {
  if (isLoading) {
    return (
      <Box sx={{ padding: 2, textAlign: "center" }}>
        <Typography color="textSecondary">Loading events...</Typography>
      </Box>
    );
  }

  if (!events || events.length === 0) {
    return (
      <Box sx={{ padding: 2, textAlign: "center" }}>
        <Typography color="textSecondary">{emptyMessage}</Typography>
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
            <TableCell>Source</TableCell>
            <TableCell>Object</TableCell>
            <TableCell>Reason</TableCell>
            <TableCell>Message</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {events.map((event, index) => (
            <TableRow key={event.sourceEventId || index}>
              <TableCell>{formatDateFromTimeMs(event.timestampNs / 1e6)}</TableCell>
              <TableCell>
                <SeverityChip severity={event.severity} />
              </TableCell>
              <TableCell>
                {event.source?.platform}/{event.source?.component}
              </TableCell>
              <TableCell>
                {event.objectKind}/{event.objectName}
              </TableCell>
              <TableCell>{event.reason}</TableCell>
              <TableCell>{event.message}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableContainer>
  );
};
