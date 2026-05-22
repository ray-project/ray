import {
  Box,
  Pagination,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from "@mui/material";
import React, { ReactElement } from "react";
import { ClassNameProps } from "../../../common/props";
import { HelpInfo } from "../../../components/Tooltip";
import { useJobProgressByTaskName } from "../hook/useJobProgress";
import { MiniTaskProgressBar } from "../TaskProgressBar";

const columns: { label: string; helpInfo?: ReactElement }[] = [
  { label: "Task name" },
  { label: "Failed" },
  { label: "Active" },
  { label: "Finished" },
  { label: "Tasks" },
];

export type JobTaskNameProgressTableProps = {
  jobId: string;
} & ClassNameProps;

export const JobTaskNameProgressTable = ({
  jobId,
  className,
}: JobTaskNameProgressTableProps) => {
  const { progress, page, setPage, total, totalTasks = 0 } = useJobProgressByTaskName(jobId);

  // Check if the number of tasks exceeds a certain threshold
  const isTruncated = total > 10000;

  return (
    <TableContainer className={className}>
      {isTruncated && (
        <Box mt={2} mb={2} display="flex" justifyContent="center">
          <Typography color="error">
            Warning: This job has truncated tasks. Only the first 10,000 tasks are displayed.
          </Typography>
        </Box>
      )}
      <div>
        <Pagination
          count={Math.ceil(total / page.pageSize)}
          page={page.pageNo}
          onChange={(_, pageNo) => setPage(pageNo)}
        />
      </div>
      <Table>
        <TableHead>
          <TableRow>
            {columns.map(({ label, helpInfo }) => (
              <TableCell align="center" key={label}>
                <Box display="flex" justifyContent="center" alignItems="center">
                  {label}
                  {helpInfo && (
                    <HelpInfo sx={{ marginLeft: 1 }}>{helpInfo}</HelpInfo>
                  )}
                </Box>
              </TableCell>
            ))}
          </TableRow>
        </TableHead>
        <TableBody>
          {progress.map(
            ({ name, progress, numFailed, numActive, numFinished }) => {
              return (
                <TableRow key={name}>
                  <TableCell align="center">{name}</TableCell>
                  <TableCell align="center">{numFailed}</TableCell>
                  <TableCell align="center">{numActive}</TableCell>
                  <TableCell align="center">{numFinished}</TableCell>
                  <TableCell>
                    <MiniTaskProgressBar {...progress} />
                  </TableCell>
                </TableRow>
              );
            },
          )}
        </TableBody>
      </Table>
    </TableContainer>
  );
};