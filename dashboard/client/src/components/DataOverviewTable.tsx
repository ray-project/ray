import {
  Box,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TextField,
  TextFieldProps,
  Typography,
} from "@material-ui/core";
import Autocomplete from "@material-ui/lab/Autocomplete";
import Pagination from "@material-ui/lab/Pagination";
import React, { useState } from "react";
import { formatDateFromTimeMs } from "../common/formatUtils";
import rowStyles from "../common/RowStyles";
import { TaskProgressBar } from "../pages/job/TaskProgressBar";
import { DatasetMetrics } from "../type/data";
import { memoryConverter } from "../util/converter";
import { useFilter } from "../util/hook";
import StateCounter from "./StatesCounter";
import { StatusChip } from "./StatusChip";
import { HelpInfo } from "./Tooltip";

const columns = [
  { label: "Dataset" },
  {
    label: "Progress",
    helpInfo: <Typography>Blocks outputted by output operator.</Typography>,
  },
  { label: "State" },
  { label: "Bytes Outputted" },
  {
    label: "Memory Usage (Current / Max)",
    helpInfo: (
      <Typography>
        Amount of object store memory used by a dataset. Includes spilled
        objects.
      </Typography>
    ),
  },
  {
    label: "Bytes Spilled",
    helpInfo: (
      <Typography>
        Set
        "ray.data.context.DataContext.get_current().enable_get_object_locations_for_metrics
        = True" to collect spill stats.
      </Typography>
    ),
  },
  { label: "Start Time" },
  { label: "End Time" },
];

const DataOverviewTable = ({
  datasets = [],
}: {
  datasets: DatasetMetrics[];
}) => {
  const [pageNo, setPageNo] = useState(1);
  const { changeFilter, filterFunc } = useFilter();
  const pageSize = 10;
  const datasetList = datasets.filter(filterFunc);

  const list = datasetList.slice((pageNo - 1) * pageSize, pageNo * pageSize);

  const classes = rowStyles();

  return (
    <div>
      <div style={{ flex: 1, display: "flex", alignItems: "center" }}>
        <Autocomplete
          style={{ margin: 8, width: 120 }}
          options={Array.from(new Set(datasets.map((e) => e.dataset)))}
          onInputChange={(_: any, value: string) => {
            changeFilter("dataset", value.trim());
          }}
          renderInput={(params: TextFieldProps) => (
            <TextField {...params} label="Dataset" />
          )}
        />
      </div>
      <div style={{ display: "flex", alignItems: "center" }}>
        <div>
          <Pagination
            page={pageNo}
            onChange={(e, num) => setPageNo(num)}
            count={Math.ceil(datasetList.length / pageSize)}
          />
        </div>
        <div>
          <StateCounter type="task" list={datasetList} />
        </div>
      </div>
      <div className={classes.tableContainer}>
        <Table>
          <TableHead>
            <TableRow>
              {columns.map(({ label, helpInfo }) => (
                <TableCell align="center" key={label}>
                  <Box
                    display="flex"
                    justifyContent="center"
                    alignItems="center"
                  >
                    {label}
                    {helpInfo && (
                      <HelpInfo className={classes.helpInfo}>
                        {helpInfo}
                      </HelpInfo>
                    )}
                  </Box>
                </TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {list.map(
              ({
                dataset,
                state,
                ray_data_current_bytes,
                ray_data_output_bytes,
                ray_data_spilled_bytes,
                progress,
                total,
                start_time,
                end_time,
              }) => (
                <TableRow key={dataset}>
                  <TableCell align="center">{dataset}</TableCell>
                  <TableCell align="center">
                    <TaskProgressBar
                      numFinished={progress}
                      numRunning={
                        state === "RUNNING" ? total - progress : undefined
                      }
                      numCancelled={
                        state === "FAILED" ? total - progress : undefined
                      }
                      total={total}
                    />
                  </TableCell>
                  <TableCell align="center">
                    <StatusChip type="task" status={state} />
                  </TableCell>
                  <TableCell align="center">
                    {memoryConverter(Number(ray_data_output_bytes.max))}
                  </TableCell>
                  <TableCell align="center">
                    {memoryConverter(Number(ray_data_current_bytes.value))}/
                    {memoryConverter(Number(ray_data_current_bytes.max))}
                  </TableCell>
                  <TableCell align="center">
                    {memoryConverter(Number(ray_data_spilled_bytes.max))}
                  </TableCell>
                  <TableCell align="center">
                    {formatDateFromTimeMs(start_time * 1000)}
                  </TableCell>
                  <TableCell align="center">
                    {end_time && formatDateFromTimeMs(end_time * 1000)}
                  </TableCell>
                </TableRow>
              ),
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  );
};

export default DataOverviewTable;
