import { Box, Link, TableCell, TableRow, Tooltip } from "@mui/material";
import React from "react";
import { Link as RouterLink } from "react-router-dom";
import { CodeDialogButtonWithPreview } from "../../common/CodeDialogButton";
import { DurationText } from "../../common/DurationText";
import { formatDateFromTimeMs } from "../../common/formatUtils";
import { SubmitStatusWithIcon } from "../../common/JobStatus";
import { SubmitInfo } from "../../type/submit";

type SubmitRowProps = {
  submit: SubmitInfo;
};

export const SubmitRow = ({ submit }: SubmitRowProps) => {
  const { submission_id, message, start_time, end_time, entrypoint } = submit;

  return (
    <TableRow>
      <TableCell align="center">
        <Link component={RouterLink} to={submission_id}>
          {submission_id}
        </Link>
      </TableCell>
      <TableCell align="center">
        <Tooltip title={entrypoint} arrow>
          <Box
            sx={{
              display: "block",
              margin: "auto",
              maxWidth: 360,
              textOverflow: "ellipsis",
              overflow: "hidden",
              whiteSpace: "nowrap",
            }}
          >
            {entrypoint}
          </Box>
        </Tooltip>
      </TableCell>
      <TableCell align="center">
        <SubmitStatusWithIcon submit={submit} />
      </TableCell>
      <TableCell align="center">
        {message ? (
          <CodeDialogButtonWithPreview
            sx={{
              maxWidth: 250,
              display: "inline-flex",
            }}
            title="Status message"
            code={message}
          />
        ) : (
          "-"
        )}
      </TableCell>
      <TableCell align="center">
        {start_time && start_time > 0 ? (
          <DurationText startTime={start_time} endTime={end_time} />
        ) : (
          "-"
        )}
      </TableCell>
      <TableCell align="center">
        <Link component={RouterLink} to={submission_id}>
          Log
        </Link>
      </TableCell>
      <TableCell align="center">
        {start_time ? formatDateFromTimeMs(start_time) : "-"}
      </TableCell>
      <TableCell align="center">
        {end_time && end_time > 0 ? formatDateFromTimeMs(end_time) : "-"}
      </TableCell>
    </TableRow>
  );
};
