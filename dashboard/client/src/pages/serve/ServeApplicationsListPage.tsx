import {
  Box,
  createStyles,
  InputAdornment,
  makeStyles,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  TextFieldProps,
  Typography,
} from "@material-ui/core";
import { Alert, Autocomplete, Pagination } from "@material-ui/lab";
import React, { ReactElement } from "react";
import { CollapsibleSection } from "../../common/CollapsibleSection";
import Loading from "../../components/Loading";
import { MetadataSection } from "../../components/MetadataSection";
import { HelpInfo } from "../../components/Tooltip";
import { useServeApplications } from "./hook/useServeApplications";
import { ServeApplicationRow } from "./ServeApplicationRow";

const useStyles = makeStyles((theme) =>
  createStyles({
    table: {
      tableLayout: "fixed",
    },
    serveInstanceWarning: {
      marginBottom: theme.spacing(2),
    },
    helpInfo: {
      marginLeft: theme.spacing(1),
    },
  }),
);

const columns: { label: string; helpInfo?: ReactElement; width?: string }[] = [
  { label: "Application name" },
  { label: "Route prefix" },
  { label: "Status" },
  { label: "Status message", width: "30%" },
  { label: "Num deployments" },
  { label: "Last deployed at" },
  { label: "Duration (since last deploy)" },
  { label: "Application config" },
];

export const ServeApplicationsListPage = () => {
  const classes = useStyles();
  const {
    serveDetails,
    serveApplicationsList,
    error,
    unfilteredList,
    page,
    setPage,
    changeFilter,
  } = useServeApplications();

  if (error) {
    return <Typography color="error">{error.toString()}</Typography>;
  }

  if (serveDetails === undefined) {
    return <Loading loading={true} />;
  }

  return (
    <div>
      <CollapsibleSection title="Serve details" startExpanded>
        {serveDetails.host && serveDetails.port ? (
          <MetadataSection
            metadataList={[
              {
                label: "Host",
                content: {
                  copyableValue: serveDetails.host,
                  value: serveDetails.host,
                },
              },
              {
                label: "Port",
                content: {
                  copyableValue: `${serveDetails.port}`,
                  value: `${serveDetails.port}`,
                },
              },
            ]}
          />
        ) : (
          <Alert className={classes.serveInstanceWarning} severity="warning">
            Serve instance not started yet. Please deploy a serve application
            first.
          </Alert>
        )}
      </CollapsibleSection>
      <CollapsibleSection title="Serve applications" startExpanded>
        <TableContainer>
          <div style={{ flex: 1, display: "flex", alignItems: "center" }}>
            <Autocomplete
              style={{ margin: 8, width: 120 }}
              options={Array.from(
                new Set(unfilteredList.map((e) => (e.name ? e.name : "-"))),
              )}
              onInputChange={(_: any, value: string) => {
                changeFilter("name", value.trim() !== "-" ? value.trim() : "");
              }}
              renderInput={(params: TextFieldProps) => (
                <TextField {...params} label="Name" />
              )}
            />
            <Autocomplete
              style={{ margin: 8, width: 120 }}
              options={Array.from(new Set(unfilteredList.map((e) => e.status)))}
              onInputChange={(_: any, value: string) => {
                changeFilter("status", value.trim());
              }}
              renderInput={(params: TextFieldProps) => (
                <TextField {...params} label="Status" />
              )}
            />
            <TextField
              style={{ margin: 8, width: 120 }}
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
          </div>
          <div style={{ display: "flex", alignItems: "center" }}>
            <Pagination
              count={Math.ceil(serveApplicationsList.length / page.pageSize)}
              page={page.pageNo}
              onChange={(e, pageNo) => setPage("pageNo", pageNo)}
            />
          </div>
          <Table className={classes.table}>
            <TableHead>
              <TableRow>
                {columns.map(({ label, helpInfo, width }) => (
                  <TableCell
                    align="center"
                    key={label}
                    style={width ? { width } : undefined}
                  >
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
              {serveApplicationsList
                .slice(
                  (page.pageNo - 1) * page.pageSize,
                  page.pageNo * page.pageSize,
                )
                .map((application) => (
                  <ServeApplicationRow
                    key={application.name}
                    application={application}
                  />
                ))}
            </TableBody>
          </Table>
        </TableContainer>
      </CollapsibleSection>
    </div>
  );
};
