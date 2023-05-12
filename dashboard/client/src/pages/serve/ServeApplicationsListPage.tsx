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
import { HelpInfo } from "../../components/Tooltip";
import { useServeApplications } from "./hook/useServeApplications";
import { ServeApplicationRow } from "./ServeApplicationRow";
import { ServeMetricsSection } from "./ServeMetricsSection";
import { ServeSystemDetails } from "./ServeSystemDetails";

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
    applicationsSection: {
      marginTop: theme.spacing(4),
    },
    metricsSection: {
      marginTop: theme.spacing(4),
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
    filteredServeApplications,
    httpProxies,
    error,
    allServeApplications,
    page,
    setPage,
    httpProxiesPage,
    setHttpProxiesPage,
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
      {serveDetails.http_options === undefined ? (
        <Alert className={classes.serveInstanceWarning} severity="warning">
          Serve not started. Please deploy a serve application first.
        </Alert>
      ) : (
        <React.Fragment>
          <ServeSystemDetails
            serveDetails={serveDetails}
            httpProxies={httpProxies}
            page={httpProxiesPage}
            setPage={setHttpProxiesPage}
          />
          <CollapsibleSection
            title="Serve applications"
            startExpanded
            className={classes.applicationsSection}
          >
            <TableContainer>
              <div style={{ flex: 1, display: "flex", alignItems: "center" }}>
                <Autocomplete
                  style={{ margin: 8, width: 120 }}
                  options={Array.from(
                    new Set(
                      allServeApplications.map((e) => (e.name ? e.name : "-")),
                    ),
                  )}
                  onInputChange={(_: any, value: string) => {
                    changeFilter(
                      "name",
                      value.trim() !== "-" ? value.trim() : "",
                    );
                  }}
                  renderInput={(params: TextFieldProps) => (
                    <TextField {...params} label="Name" />
                  )}
                />
                <Autocomplete
                  style={{ margin: 8, width: 120 }}
                  options={Array.from(
                    new Set(allServeApplications.map((e) => e.status)),
                  )}
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
                  count={Math.ceil(
                    filteredServeApplications.length / page.pageSize,
                  )}
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
                  {filteredServeApplications
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
        </React.Fragment>
      )}
      <ServeMetricsSection className={classes.metricsSection} />
    </div>
  );
};
