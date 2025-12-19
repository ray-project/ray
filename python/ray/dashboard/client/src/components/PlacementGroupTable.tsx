import {
  Box,
  InputAdornment,
  SxProps,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TextField,
  TextFieldProps,
  Theme,
  Tooltip,
} from "@mui/material";
import Autocomplete from "@mui/material/Autocomplete";
import Pagination from "@mui/material/Pagination";
import React, { useState } from "react";
import { CodeDialogButtonWithPreview } from "../common/CodeDialogButton";
import rowStyles from "../common/RowStyles";
import { sliceToPage } from "../common/util";
import { Bundle, PlacementGroup } from "../type/placementGroup";
import { useFilter } from "../util/hook";
import StateCounter from "./StatesCounter";
import { StatusChip } from "./StatusChip";

const BundleResourceRequirements = ({
  bundles,
}: {
  bundles: Bundle[];
  sx?: SxProps<Theme>;
}) => {
  const resources = bundles.map(({ unit_resources }) => unit_resources);
  return (
    <React.Fragment>
      {Object.entries(resources).length > 0 ? (
        <CodeDialogButtonWithPreview
          title="Required resources"
          code={JSON.stringify(resources, undefined, 2)}
        />
      ) : (
        "[]"
      )}
    </React.Fragment>
  );
};

const LabelSelector = ({
  bundles,
}: {
  bundles: Bundle[];
  sx?: SxProps<Theme>;
}) => {
  const labelSelector = bundles.map(({ label_selector }) => label_selector);
  return (
    <React.Fragment>
      {Object.entries(labelSelector).length > 0 ? (
        <CodeDialogButtonWithPreview
          title="Label selector"
          code={JSON.stringify(labelSelector, undefined, 2)}
        />
      ) : (
        "[]"
      )}
    </React.Fragment>
  );
};

const PlacementGroupTable = ({
  placementGroups = [],
  jobId = null,
}: {
  placementGroups: PlacementGroup[];
  jobId?: string | null;
}) => {
  const [pageNo, setPageNo] = useState(1);
  const { changeFilter, filterFunc } = useFilter();
  const [pageSize, setPageSize] = useState(10);
  const placementGroupList = placementGroups.filter(filterFunc);
  const {
    items: list,
    constrainedPage,
    maxPage,
  } = sliceToPage(placementGroupList, pageNo, pageSize);

  const columns = [
    { label: "ID" },
    { label: "Name" },
    { label: "Job Id" },
    { label: "State" },
    { label: "Reserved Resources" },
    { label: "Label Selector" },
    { label: "Scheduling Detail" },
  ];

  return (
    <div>
      <div style={{ flex: 1, display: "flex", alignItems: "center" }}>
        <Autocomplete
          style={{ margin: 8, width: 120 }}
          options={Array.from(
            new Set(placementGroups.map((e) => e.placement_group_id)),
          )}
          onInputChange={(_: any, value: string) => {
            changeFilter("placement_group_id", value.trim());
          }}
          renderInput={(params: TextFieldProps) => (
            <TextField {...params} label="Placement group ID" />
          )}
        />
        <Autocomplete
          style={{ margin: 8, width: 120 }}
          options={Array.from(new Set(placementGroups.map((e) => e.state)))}
          onInputChange={(_: any, value: string) => {
            changeFilter("state", value.trim());
          }}
          renderInput={(params: TextFieldProps) => (
            <TextField {...params} label="State" />
          )}
        />
        <Autocomplete
          style={{ margin: 8, width: 150 }}
          defaultValue={jobId}
          options={Array.from(
            new Set(placementGroups.map((e) => e.creator_job_id)),
          )}
          onInputChange={(_: any, value: string) => {
            changeFilter("creator_job_id", value.trim());
          }}
          renderInput={(params: TextFieldProps) => (
            <TextField {...params} label="Job Id" />
          )}
        />
        <Autocomplete
          style={{ margin: 8, width: 150 }}
          options={Array.from(new Set(placementGroups.map((e) => e.name)))}
          onInputChange={(_: any, value: string) => {
            changeFilter("name", value.trim());
          }}
          renderInput={(params: TextFieldProps) => (
            <TextField {...params} label="Name" />
          )}
        />
        <TextField
          label="Page Size"
          sx={{ margin: 1, width: 120 }}
          size="small"
          defaultValue={10}
          InputProps={{
            onChange: ({ target: { value } }) => {
              setPageSize(Math.min(Number(value), 500) || 10);
            },
            endAdornment: (
              <InputAdornment position="end">Per Page</InputAdornment>
            ),
          }}
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
          <StateCounter type="placementGroup" list={placementGroupList} />
        </div>
      </div>
      <Box sx={{ overflowX: "scroll" }}>
        <Table>
          <TableHead>
            <TableRow>
              {columns.map(({ label }) => (
                <TableCell align="center" key={label}>
                  <Box
                    display="flex"
                    justifyContent="center"
                    alignItems="center"
                  >
                    {label}
                  </Box>
                </TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {list.map(
              ({
                placement_group_id,
                name,
                creator_job_id,
                state,
                stats,
                bundles,
              }) => (
                <TableRow key={placement_group_id}>
                  <TableCell align="center">
                    <Tooltip title={placement_group_id} arrow>
                      <Box sx={rowStyles.idCol}>{placement_group_id}</Box>
                    </Tooltip>
                  </TableCell>
                  <TableCell align="center">{name ? name : "-"}</TableCell>
                  <TableCell align="center">{creator_job_id}</TableCell>
                  <TableCell align="center">
                    <StatusChip type="placementGroup" status={state} />
                  </TableCell>
                  <TableCell align="center">
                    <BundleResourceRequirements bundles={bundles} />
                  </TableCell>
                  <TableCell align="center">
                    <LabelSelector bundles={bundles} />
                  </TableCell>
                  <TableCell align="center">
                    {stats ? stats.scheduling_state : "-"}
                  </TableCell>
                </TableRow>
              ),
            )}
          </TableBody>
        </Table>
      </Box>
    </div>
  );
};

export default PlacementGroupTable;
