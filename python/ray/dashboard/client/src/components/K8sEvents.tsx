import React, { useEffect, useState } from "react";
import { Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Paper, Chip, Box, Typography } from "@mui/material";
import { getK8sEvents, PlatformEvent } from "../service/k8s_events";

type K8sEventsProps = {
  nodeId?: string;
};

export const K8sEvents = ({ nodeId }: K8sEventsProps) => {
  const [events, setEvents] = useState<PlatformEvent[]>([]);

  useEffect(() => {
    const fetchEvents = async () => {
      try {
        const data = await getK8sEvents();
        // Sort by timestamp descending
        let sorted = (data || []).sort((a, b) => Number(b.timestampMs) - Number(a.timestampMs));

        if (nodeId) {
          sorted = sorted.filter(e => e.relatedNodeId === nodeId);
        }

        setEvents(sorted);
      } catch (e) {
        console.error("Failed to fetch k8s events", e);
      }
    };
    fetchEvents();
    const interval = setInterval(fetchEvents, 4000);
    return () => clearInterval(interval);
  }, [nodeId]);

  return (
    <TableContainer component={Paper}>
      <Table sx={{ minWidth: 650 }} aria-label="k8s events table">
        <TableHead>
          <TableRow>
            <TableCell>Timestamp</TableCell>
            <TableCell>Severity</TableCell>
            <TableCell>Source</TableCell>
            <TableCell>Node ID</TableCell>
            <TableCell>Message</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {events.map((event, index) => (
            <TableRow key={index}>
              <TableCell>{new Date(Number(event.timestampMs)).toLocaleString()}</TableCell>
              <TableCell>
                <Chip
                  label={event.severity}
                  size="small"
                  color={
                    event.severity === "ERROR"
                      ? "error"
                      : event.severity === "WARNING"
                        ? "warning"
                        : "default"
                  }
                  sx={{ fontWeight: 500 }}
                />
              </TableCell>
              <TableCell>{event.source}</TableCell>
              <TableCell>{event.relatedNodeId || "-"}</TableCell>
              <TableCell>{event.message}</TableCell>
            </TableRow>
          ))}
          {events.length === 0 && (
            <TableRow>
              <TableCell colSpan={5} align="center">
                <Typography color="textSecondary">
                  No Kubernetes events found{nodeId ? ` for node ${nodeId}` : ""}.
                </Typography>
              </TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>
    </TableContainer>
  );
};
