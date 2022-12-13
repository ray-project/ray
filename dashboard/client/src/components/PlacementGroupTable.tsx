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
} from "@material-ui/core";
import Autocomplete from "@material-ui/lab/Autocomplete";
import Pagination from "@material-ui/lab/Pagination";
import React, { useState } from "react";
import rowStyles from "../common/RowStyles";
import { PlacementGroup } from "../type/placementGroup";
import { useFilter } from "../util/hook";
import StateCounter from "./StatesCounter";
import { StatusChip } from "./StatusChip";

const PlacementGroupTable = ({
  placementGroups = [],
  jobId = null,
}: {
  placementGroups: Array<PlacementGroup>;
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
    { label: "Scheduling Detail" },
  ];

  return (
    <React.Fragment>
      <div style={{ flex: 1, display: "flex", alignItems: "center" }}>
        <Autocomplete
          style={{ margin: 8, width: 120 }}
          options={Array.from(
            new Set(placementGroups.map((e) => e.placement_group_id)),
          )}
          onInputChange={(_: any, value: string) => {
            changeFilter("state", value.trim());
          }}
          renderInput={(params: TextFieldProps) => (
            <TextField {...params} label="Placement group ID" />
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
          style={{ margin: 8, width: 120 }}
          label="Page Size"
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
      <Table>
        <TableHead>
          <TableRow>
            {columns.map(({ label }) => (
              <TableCell align="center" key={label}>
                <Box display="flex" justifyContent="center" alignItems="center">
                  {label}
                </Box>
              </TableCell>
            ))}
          </TableRow>
        </TableHead>
        <TableBody>
          {list.map(
            ({ placement_group_id, name, creator_job_id, state, stats }) => (
              <TableRow>
                <TableCell align="center">
                  <Tooltip
                    className={classes.idCol}
                    title={placement_group_id}
                    arrow
                    interactive
                  >
                    <div>{placement_group_id}</div>
                  </Tooltip>
                </TableCell>
                <TableCell align="center">{name ? name : "-"}</TableCell>
                <TableCell align="center">{creator_job_id}</TableCell>
                <TableCell align="center">
                  <StatusChip type="actor" status={state} />
                </TableCell>
                <TableCell align="center">
                  {stats ? stats.scheduling_state : "-"}
                </TableCell>
              </TableRow>
            ),
          )}
        </TableBody>
      </Table>
    </React.Fragment>
  );
};

export default PlacementGroupTable;
