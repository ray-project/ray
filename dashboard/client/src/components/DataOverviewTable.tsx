import {
  Box,
  createStyles,
  makeStyles,
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
import { RiArrowDownSLine, RiArrowRightSLine } from "react-icons/ri";
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
  const [expanded, setExpanded] = useState({});

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
              {columns.map(({ label, helpInfo }, i) => (
                <TableCell align="center" key={label}>
                  <Box
                    display="flex"
                    justifyContent={i === 0 ? "start" : "end"}
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
            {list.map((dataset, index) => (
              <DatasetTable
                datasetMetrics={dataset}
                isExpanded={expanded[dataset.dataset as keyof {}]}
                setIsExpanded={(isExpanded: boolean) => {
                  const copy = { ...expanded, [dataset.dataset]: isExpanded };
                  setExpanded(copy);
                }}
                key={dataset.dataset}
              />
            ))}
          </TableBody>
        </Table>
      </div>
    </div>
  );
};

const useStyles = makeStyles((theme) =>
  createStyles({
    icon: {
      width: 16,
      height: 16,
    },
  }),
);

const DatasetTable = ({
  datasetMetrics,
  isExpanded,
  setIsExpanded,
}: {
  datasetMetrics: DatasetMetrics;
  isExpanded: boolean;
  setIsExpanded: CallableFunction;
}) => {
  const classes = useStyles();
  const {
    dataset,
    state,
    progress,
    total,
    ray_data_current_bytes,
    ray_data_output_bytes,
    ray_data_spilled_bytes,
    start_time,
    end_time,
    operators,
  } = datasetMetrics;
  const operatorRows =
    isExpanded &&
    operators.map((operator) => (
      <TableRow key={operator.operator}>
        <TableCell align="left">{operator.operator}</TableCell>
        <TableCell align="right">
          <TaskProgressBar
            numFinished={operator.progress}
            numRunning={
              operator.state === "RUNNING"
                ? operator.total - operator.progress
                : undefined
            }
            numCancelled={
              operator.state === "FAILED"
                ? operator.total - operator.progress
                : undefined
            }
            total={operator.total}
          />
        </TableCell>
        <TableCell align="right">
          <StatusChip type="task" status={operator.state} />
        </TableCell>
        <TableCell align="right">
          {memoryConverter(Number(operator.ray_data_output_bytes.max))}
        </TableCell>
        <TableCell align="right">
          {memoryConverter(Number(operator.ray_data_current_bytes.value))}/
          {memoryConverter(Number(operator.ray_data_current_bytes.max))}
        </TableCell>
        <TableCell align="right">
          {memoryConverter(Number(operator.ray_data_spilled_bytes.max))}
        </TableCell>
        <TableCell></TableCell>
        <TableCell></TableCell>
      </TableRow>
    ));
  return (
    <React.Fragment>
      <TableRow key={dataset}>
        <TableCell align="left">
          <div style={{ display: "flex" }}>
            {isExpanded ? (
              <RiArrowDownSLine
                title={"Collapse Dataset " + dataset}
                className={classes.icon}
                onClick={() => setIsExpanded(false)}
              />
            ) : (
              <RiArrowRightSLine
                title={"Expand Dataset " + dataset}
                className={classes.icon}
                onClick={() => setIsExpanded(true)}
              />
            )}
            {dataset}
          </div>
        </TableCell>
        <TableCell align="right">
          <TaskProgressBar
            numFinished={progress}
            numRunning={state === "RUNNING" ? total - progress : undefined}
            numCancelled={state === "FAILED" ? total - progress : undefined}
            total={total}
          />
        </TableCell>
        <TableCell align="right">
          <StatusChip type="task" status={state} />
        </TableCell>
        <TableCell align="right">
          {memoryConverter(Number(ray_data_output_bytes.max))}
        </TableCell>
        <TableCell align="right">
          {memoryConverter(Number(ray_data_current_bytes.value))}/
          {memoryConverter(Number(ray_data_current_bytes.max))}
        </TableCell>
        <TableCell align="right">
          {memoryConverter(Number(ray_data_spilled_bytes.max))}
        </TableCell>
        <TableCell align="right">
          {formatDateFromTimeMs(start_time * 1000)}
        </TableCell>
        <TableCell align="right">
          {end_time && formatDateFromTimeMs(end_time * 1000)}
        </TableCell>
      </TableRow>
      {operatorRows}
    </React.Fragment>
  );
};

export default DataOverviewTable;
