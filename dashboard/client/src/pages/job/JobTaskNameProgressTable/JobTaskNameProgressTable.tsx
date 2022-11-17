import {
  Box,
  createStyles,
  makeStyles,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
} from "@material-ui/core";
import { Pagination } from "@material-ui/lab";
import React, { ReactElement } from "react";
import { ClassNameProps } from "../../../common/props";
import { HelpInfo } from "../../../components/Tooltip";
import { useJobProgressByTaskName } from "../hook/useJobProgress";
import { MiniTaskProgressBar } from "../TaskProgressBar";

const useStyles = makeStyles((theme) =>
  createStyles({
    helpInfo: {
      marginLeft: theme.spacing(1),
    },
  }),
);

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
  const classes = useStyles();

  const { progress, page, setPage, total } = useJobProgressByTaskName(jobId);

  return (
    <TableContainer className={className}>
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
                    <HelpInfo className={classes.helpInfo}>{helpInfo}</HelpInfo>
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
