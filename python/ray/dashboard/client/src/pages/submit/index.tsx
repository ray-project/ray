import {
  Box,
  InputAdornment,
  Switch,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
} from "@mui/material";
import Autocomplete from "@mui/material/Autocomplete";
import Pagination from "@mui/material/Pagination";
import React from "react";
import { Outlet } from "react-router-dom";
import { sliceToPage } from "../../common/util";
import Loading from "../../components/Loading";
import { SearchInput } from "../../components/SearchComponent";
import TitleCard from "../../components/TitleCard";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { useSubmitList } from "./hook/useSubmitList";
import { SubmitRow } from "./SubmitRow";

const columns = [
  { label: "Submission ID" },
  { label: "Entrypoint" },
  { label: "Status" },
  { label: "Status message" },
  { label: "Duration" },
  { label: "Actions" },
  { label: "StartTime" },
  { label: "EndTime" },
];

const SubmissionList = () => {
  const {
    msg,
    isLoading,
    isRefreshing,
    onSwitchChange,
    submitList,
    changeFilter,
    page,
    setPage,
  } = useSubmitList();

  const {
    items: list,
    constrainedPage,
    maxPage,
  } = sliceToPage(submitList, page.pageNo, page.pageSize);

  return (
    <Box sx={{ padding: 2, width: "100%" }}>
      <Loading loading={isLoading} />
      <TitleCard title="SUBMIT">
        Auto Refresh:
        <Switch
          checked={isRefreshing}
          onChange={(event) => {
            onSwitchChange(event);
          }}
          name="refresh"
          inputProps={{ "aria-label": "secondary checkbox" }}
        />
        <br />
        Request Status: {msg}
      </TitleCard>
      <TitleCard title="Submission List">
        <TableContainer>
          <Box
            sx={{
              display: "flex",
              alignItems: "center",
              gap: 2,
              paddingTop: 1,
            }}
          >
            <SearchInput
              label="Submit ID"
              onChange={(value) => changeFilter("submission_id", value)}
            />
            <TextField
              sx={{ width: 120 }}
              label="Page Size"
              size="small"
              defaultValue={10}
              InputProps={{
                onChange: ({ target: { value } }) => {
                  setPage("pageSize", Math.min(Number(value), 500) || 10);
                },
                endAdornment: (
                  <InputAdornment position="end">Per Page</InputAdornment>
                ),
              }}
            />
            <Autocomplete
              sx={{ height: 35, width: 150 }}
              options={["PENDING", "RUNNING", "SUCCEEDED", "FAILED"]}
              onInputChange={(event, value) =>
                changeFilter("status", value.trim())
              }
              renderInput={(params) => <TextField {...params} label="Status" />}
            />
          </Box>
          <div>
            <Pagination
              count={maxPage}
              page={constrainedPage}
              onChange={(e, pageNo) => setPage("pageNo", pageNo)}
            />
          </div>
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
              {list.map((submit, index) => {
                const submission_id = submit.submission_id;
                return <SubmitRow key={submission_id} submit={submit} />;
              })}
            </TableBody>
          </Table>
        </TableContainer>
      </TitleCard>
    </Box>
  );
};

/**
 * Jobs page for the new information hierarchy
 */
export const SubmissionLayout = () => {
  return (
    <React.Fragment>
      <MainNavPageInfo
        pageInfo={{
          title: "Submit",
          id: "submit",
          path: "/submit",
        }}
      />
      <Outlet />
    </React.Fragment>
  );
};
export default SubmissionList;
