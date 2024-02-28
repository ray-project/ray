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
import { useServeDeployments } from "./hook/useServeApplications";
import { ServeDeploymentRow } from "./ServeDeploymentRow";
import { ServeEntityLogViewer } from "./ServeEntityLogViewer";
import { ServeMetricsSection } from "./ServeMetricsSection";
import { ServeSystemPreview } from "./ServeSystemDetails";

const useStyles = makeStyles((theme) =>
  createStyles({
    root: {
      padding: theme.spacing(3),
    },
    table: {
      tableLayout: "fixed",
    },
    serveInstanceWarning: {
      marginBottom: theme.spacing(2),
    },
    helpInfo: {
      marginLeft: theme.spacing(1),
    },
    deploymentsSection: {
      marginTop: theme.spacing(4),
    },
    section: {
      marginTop: theme.spacing(4),
    },
  }),
);

const columns: { label: string; helpInfo?: ReactElement; width?: string }[] = [
  { label: "Deployment name" },
  { label: "Status" },
  { label: "Status message", width: "30%" },
  { label: "Num replicas" },
  { label: "Actions" },
  { label: "Application" },
  { label: "Route prefix" },
  { label: "Last deployed at" },
  { label: "Duration (since last deploy)" },
];

export const ServeDeploymentsListPage = () => {
  const classes = useStyles();
  const {
    serveDetails,
    filteredServeDeployments,
    error,
    allServeDeployments,
    page,
    setPage,
    proxies,
    changeFilter,
  } = useServeDeployments();

  if (error) {
    return <Typography color="error">{error.toString()}</Typography>;
  }

  if (serveDetails === undefined) {
    return <Loading loading={true} />;
  }

  return (
    <div className={classes.root}>
      {serveDetails.http_options === undefined ? (
        <Alert className={classes.serveInstanceWarning} severity="warning">
          Serve not started. Please deploy a serve application first.
        </Alert>
      ) : (
        <React.Fragment>
          <ServeSystemPreview
            allDeployments={allServeDeployments}
            proxies={proxies}
            serveDetails={serveDetails}
          />
          <CollapsibleSection
            title="Deployments"
            startExpanded
            className={classes.deploymentsSection}
          >
            <TableContainer>
              <div style={{ flex: 1, display: "flex", alignItems: "center" }}>
                <Autocomplete
                  style={{ margin: 8, width: 120 }}
                  options={Array.from(
                    new Set(
                      allServeDeployments.map((e) => (e.name ? e.name : "-")),
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
                    new Set(allServeDeployments.map((e) => e.status)),
                  )}
                  onInputChange={(_: any, value: string) => {
                    changeFilter("status", value.trim());
                  }}
                  renderInput={(params: TextFieldProps) => (
                    <TextField {...params} label="Status" />
                  )}
                />
                <Autocomplete
                  style={{ margin: 8, width: 120 }}
                  options={Array.from(
                    new Set(allServeDeployments.map((e) => e.applicationName)),
                  )}
                  onInputChange={(_: any, value: string) => {
                    changeFilter("applicationName", value.trim());
                  }}
                  renderInput={(params: TextFieldProps) => (
                    <TextField {...params} label="Application" />
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
                    filteredServeDeployments.length / page.pageSize,
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
                  {filteredServeDeployments
                    .slice(
                      (page.pageNo - 1) * page.pageSize,
                      page.pageNo * page.pageSize,
                    )
                    .map((deployment) => (
                      <ServeDeploymentRow
                        key={`${deployment.application.name}-${deployment.name}`}
                        deployment={deployment}
                        application={deployment.application}
                      />
                    ))}
                </TableBody>
              </Table>
            </TableContainer>
          </CollapsibleSection>
          <CollapsibleSection
            title="Logs"
            startExpanded
            className={classes.section}
          >
            <ServeEntityLogViewer
              controller={serveDetails.controller_info}
              proxies={proxies}
              deployments={allServeDeployments}
            />
          </CollapsibleSection>
        </React.Fragment>
      )}
      <ServeMetricsSection className={classes.section} />
    </div>
  );
};
