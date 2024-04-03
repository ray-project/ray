import {
  Box,
  InputAdornment,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TextField,
  TextFieldProps,
  Tooltip,
} from "@mui/material";
import Autocomplete from "@mui/material/Autocomplete";
import Pagination from "@mui/material/Pagination";
import React, { useState } from "react";
import rowStyles from "../common/RowStyles";
import { Bundle, PlacementGroup } from "../type/placementGroup";
import { useFilter } from "../util/hook";
import StateCounter from "./StatesCounter";
import { StatusChip } from "./StatusChip";

const BundleResourceRequirements = ({ bundles }: { bundles: Bundle[] }) => {
  return (
    <div>
      {bundles.map(({ unit_resources }, index) => {
        return `{${Object.entries(unit_resources || {})
          .map(([key, val]) => `${key}: ${val}`)
          .join(", ")}}, `;
      })}
    </div>
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
  const list = placementGroupList.slice(
    (pageNo - 1) * pageSize,
    pageNo * pageSize,
  );
  const classes = rowStyles();

  const columns = [
    { label: "ID" },
    { label: "Name" },
    { label: "Job Id" },
    { label: "State" },
    { label: "Reserved Resources" },
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
            page={pageNo}
            onChange={(e, num) => setPageNo(num)}
            count={Math.ceil(placementGroupList.length / pageSize)}
          />
        </div>
        <div>
          <StateCounter type="placementGroup" list={placementGroupList} />
        </div>
      </div>
      <div className={classes.tableContainer}>
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
                    <Tooltip
                      className={classes.idCol}
                      title={placement_group_id}
                      arrow
                    >
                      <div>{placement_group_id}</div>
                    </Tooltip>
                  </TableCell>
                  <TableCell align="center">{name ? name : "-"}</TableCell>
                  <TableCell align="center">{creator_job_id}</TableCell>
                  <TableCell align="center">
                    <StatusChip type="placementGroup" status={state} />
                  </TableCell>
                  <TableCell align="center">
                    <Tooltip
                      className={classes.OverflowCol}
                      title={<BundleResourceRequirements bundles={bundles} />}
                      arrow
                    >
                      <BundleResourceRequirements bundles={bundles} />
                    </Tooltip>
                  </TableCell>
                  <TableCell align="center">
                    {stats ? stats.scheduling_state : "-"}
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

export default PlacementGroupTable;
