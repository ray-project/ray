import {
  Alert,
  Box,
  InputAdornment,
  Pagination,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  Typography,
} from "@mui/material";
import createStyles from "@mui/styles/createStyles";
import makeStyles from "@mui/styles/makeStyles";
import React, { ReactElement } from "react";
import { CollapsibleSection } from "../../common/CollapsibleSection";
import Loading from "../../components/Loading";
import { HelpInfo } from "../../components/Tooltip";
import { useServeDeployments } from "./hook/useServeApplications";
import { ServeApplicationRows } from "./ServeApplicationRow";
import { ServeEntityLogViewer } from "./ServeEntityLogViewer";
import {
  APPS_METRICS_CONFIG,
  ServeMetricsSection,
} from "./ServeMetricsSection";
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
  { label: "" }, // Empty space for expand button
  { label: "Name" },
  { label: "Status" },
  { label: "Status message", width: "30%" },
  { label: "Replicas" },
  { label: "Actions" },
  { label: "Route prefix" },
  { label: "Last deployed at" },
  { label: "Duration (since last deploy)" },
];

export const ServeDeploymentsListPage = () => {
  const classes = useStyles();
  const {
    serveDetails,
    error,
    page,
    setPage,
    proxies,
    serveApplications,
    serveDeployments,
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
            allDeployments={serveDeployments}
            allApplications={serveApplications}
            proxies={proxies}
            serveDetails={serveDetails}
          />
          <CollapsibleSection
            title="Applications / Deployments"
            startExpanded
            className={classes.deploymentsSection}
          >
            <TableContainer>
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
              <Pagination
                count={Math.ceil(serveApplications.length / page.pageSize)}
                page={page.pageNo}
                onChange={(e, pageNo) => setPage("pageNo", pageNo)}
              />
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
                  {serveApplications
                    .slice(
                      (page.pageNo - 1) * page.pageSize,
                      page.pageNo * page.pageSize,
                    )
                    .map((application) => (
                      <ServeApplicationRows
                        key={`${application.name}`}
                        application={application}
                        startExpanded
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
              deployments={serveDeployments}
            />
          </CollapsibleSection>
        </React.Fragment>
      )}
      <ServeMetricsSection
        className={classes.section}
        metricsConfig={APPS_METRICS_CONFIG}
      />
    </div>
  );
};
