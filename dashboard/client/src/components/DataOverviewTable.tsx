import {
  Box,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  TextFieldProps,
  Typography,
} from "@mui/material";
import Autocomplete from "@mui/material/Autocomplete";
import Pagination from "@mui/material/Pagination";
import React, { useState } from "react";
import { RiArrowDownSLine, RiArrowRightSLine } from "react-icons/ri";
import { formatDateFromTimeMs } from "../common/formatUtils";
import { sliceToPage } from "../common/util";
import { TaskProgressBar } from "../pages/job/TaskProgressBar";
import { DatasetMetrics, OperatorMetrics } from "../type/data";
import { memoryConverter } from "../util/converter";
import { useFilter } from "../util/hook";
import StateCounter from "./StatesCounter";
import { StatusChip } from "./StatusChip";
import { HelpInfo } from "./Tooltip";

const columns = [
  { label: "" }, // Empty column for dropdown icons
  { label: "Dataset / Operator Name", align: "start" },
  {
    label: "Blocks Outputted",
    helpInfo: <Typography>Blocks outputted by output operator.</Typography>,
  },
  { label: "State", align: "center" },
  { label: "Rows Outputted" },
  {
    label: "Memory Usage (current / max)",
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
  {
    label: "Logical CPU Cores (current / max)",
    align: "center",
  },
  {
    label: "Logical GPU Cores (current / max)",
    align: "center",
  },
  { label: "Start Time", align: "center" },
  { label: "End Time", align: "center" },
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
  const [expandedDatasets, setExpandedDatasets] = useState<
    Record<string, boolean>
  >({});

  const {
    items: list,
    constrainedPage,
    maxPage,
  } = sliceToPage(datasetList, pageNo, pageSize);

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
            <TextField {...params} label="Dataset Name" />
          )}
        />
      </div>
      <div style={{ display: "flex", alignItems: "center" }}>
        <div>
          <Pagination
            page={constrainedPage}
            onChange={(e, num) => setPageNo(num)}
            count={maxPage}
          />
        </div>
        <div>
          <StateCounter type="task" list={datasetList} />
        </div>
      </div>
      <TableContainer>
        <Table>
          <TableHead>
            <TableRow>
              {columns.map(({ label, helpInfo, align }) => (
                <TableCell align="center" key={label}>
                  <Box
                    display="flex"
                    justifyContent={align ? align : "end"}
                    alignItems="center"
                  >
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
            {list.map((dataset) => (
              <DatasetTable
                datasetMetrics={dataset}
                isExpanded={expandedDatasets[dataset.dataset]}
                setIsExpanded={(isExpanded: boolean) => {
                  const copy = {
                    ...expandedDatasets,
                    [dataset.dataset]: isExpanded,
                  };
                  setExpandedDatasets(copy);
                }}
                key={dataset.dataset}
              />
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </div>
  );
};

const DataRow = ({
  datasetMetrics,
  operatorMetrics,
  isExpanded,
  setIsExpanded,
}: {
  datasetMetrics?: DatasetMetrics;
  operatorMetrics?: OperatorMetrics;
  isExpanded?: boolean;
  setIsExpanded?: CallableFunction;
}) => {
  const isDatasetRow = datasetMetrics !== undefined;
  const isOperatorRow = operatorMetrics !== undefined;
  const data = datasetMetrics || operatorMetrics;
  if ((isDatasetRow && isOperatorRow) || data === undefined) {
    throw new Error(
      "Exactly one of datasetMetrics or operatorMetrics musts be given.",
    );
  }
  return (
    <TableRow>
      <TableCell align="center">
        {isDatasetRow &&
          setIsExpanded !== undefined &&
          (isExpanded ? (
            <Box
              component={RiArrowDownSLine}
              title={"Collapse Dataset " + datasetMetrics.dataset}
              sx={{ width: 16, height: 16 }}
              onClick={() => setIsExpanded(false)}
            />
          ) : (
            <Box
              component={RiArrowRightSLine}
              title={"Expand Dataset " + datasetMetrics.dataset}
              sx={{ width: 16, height: 16 }}
              onClick={() => setIsExpanded(true)}
            />
          ))}
      </TableCell>
      <TableCell align="left">
        {isDatasetRow && datasetMetrics.dataset}
        {isOperatorRow && operatorMetrics.operator}
      </TableCell>
      <TableCell align="right" style={{ width: 200 }}>
        <TaskProgressBar
          showLegend={false}
          numFinished={data.progress}
          numRunning={
            data.state === "RUNNING" ? data.total - data.progress : undefined
          }
          numCancelled={
            data.state === "FAILED" ? data.total - data.progress : undefined
          }
          total={data.total}
        />
      </TableCell>
      <TableCell align="center">
        <StatusChip type="task" status={data.state} />
      </TableCell>
      <TableCell align="right">{data.ray_data_output_rows.max}</TableCell>
      <TableCell align="right">
        {memoryConverter(Number(data.ray_data_current_bytes.value))}/
        {memoryConverter(Number(data.ray_data_current_bytes.max))}
      </TableCell>
      <TableCell align="right">
        {memoryConverter(Number(data.ray_data_spilled_bytes.max))}
      </TableCell>
      <TableCell align="center" style={{ width: 200 }}>
        {data.ray_data_cpu_usage_cores.value}/
        {data.ray_data_cpu_usage_cores.max}
      </TableCell>
      <TableCell align="center" style={{ width: 200 }}>
        {data.ray_data_gpu_usage_cores.value}/
        {data.ray_data_gpu_usage_cores.max}
      </TableCell>
      <TableCell align="center">
        {isDatasetRow && formatDateFromTimeMs(datasetMetrics.start_time * 1000)}
      </TableCell>
      <TableCell align="center">
        {isDatasetRow &&
          datasetMetrics.end_time &&
          formatDateFromTimeMs(datasetMetrics.end_time * 1000)}
      </TableCell>
    </TableRow>
  );
};

const DatasetTable = ({
  datasetMetrics,
  isExpanded,
  setIsExpanded,
}: {
  datasetMetrics: DatasetMetrics;
  isExpanded: boolean;
  setIsExpanded: CallableFunction;
}) => {
  const operatorRows =
    isExpanded &&
    datasetMetrics.operators.map((operator) => (
      <DataRow operatorMetrics={operator} key={operator.operator} />
    ));
  return (
    <React.Fragment>
      <DataRow
        datasetMetrics={datasetMetrics}
        isExpanded={isExpanded}
        setIsExpanded={setIsExpanded}
      />
      {operatorRows}
    </React.Fragment>
  );
};

export default DataOverviewTable;
