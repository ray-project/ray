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
import { Autocomplete, Pagination } from "@material-ui/lab";
import React, { ReactElement } from "react";
import { Outlet, useParams } from "react-router-dom";
import { CodeDialogButton } from "../../common/CodeDialogButton";
import { CollapsibleSection } from "../../common/CollapsibleSection";
import { DurationText } from "../../common/DurationText";
import { formatDateFromTimeMs } from "../../common/formatUtils";
import Loading from "../../components/Loading";
import { MetadataSection } from "../../components/MetadataSection";
import { StatusChip } from "../../components/StatusChip";
import { HelpInfo } from "../../components/Tooltip";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { useServeDeploymentDetails } from "./hook/useServeApplications";
import { ServeReplicaRow } from "./ServeDeploymentRow";
import { ServeEntityLogViewer } from "./ServeEntityLogViewer";

const useStyles = makeStyles((theme) =>
  createStyles({
    root: {
      padding: theme.spacing(3),
    },
    table: {
      tableLayout: "fixed",
    },
    helpInfo: {
      marginLeft: theme.spacing(1),
    },
    logSection: {
      marginTop: theme.spacing(4),
    },
  }),
);

const columns: { label: string; helpInfo?: ReactElement; width?: string }[] = [
  { label: "Replica ID" },
  { label: "Status" },
  { label: "Actions" },
  { label: "Started at" },
  { label: "Duration" },
];

export const ServeDeploymentDetailPage = () => {
  const classes = useStyles();
  const { applicationName, deploymentName } = useParams();

  const {
    application,
    deployment,
    filteredReplicas,
    page,
    setPage,
    changeFilter,
  } = useServeDeploymentDetails(applicationName, deploymentName);

  if (!application) {
    return (
      <Typography color="error">
        Application with name "{applicationName}" not found.
      </Typography>
    );
  }

  if (!deployment) {
    return (
      <Typography color="error">
        Deployment with name "{deploymentName}" not found.
      </Typography>
    );
  }

  return (
    <div className={classes.root}>
      <MetadataSection
        metadataList={[
          {
            label: "Status",
            content: (
              <React.Fragment>
                <StatusChip type="serveDeployment" status={deployment.status} />{" "}
                {deployment.message && (
                  <CodeDialogButton
                    title="Status details"
                    code={deployment.message}
                    buttonText="View details"
                  />
                )}
              </React.Fragment>
            ),
          },
          {
            label: "Name",
            content: {
              value: deployment.name,
            },
          },
          {
            label: "Replicas",
            content: {
              value: `${Object.keys(deployment.replicas).length}`,
            },
          },
          {
            label: "Deployment config",
            content: deployment.deployment_config ? (
              <CodeDialogButton
                title={
                  deployment.name
                    ? `Deployment config for ${deployment.name}`
                    : `Deployment config`
                }
                code={deployment.deployment_config}
              />
            ) : (
              <Typography>-</Typography>
            ),
          },
          {
            label: "Last deployed at",
            content: {
              value: formatDateFromTimeMs(
                application.last_deployed_time_s * 1000,
              ),
            },
          },
          {
            label: "Duration (since last deploy)",
            content: (
              <DurationText
                startTime={application.last_deployed_time_s * 1000}
              />
            ),
          },
        ]}
      />
      <CollapsibleSection title="Replicas" startExpanded>
        <TableContainer>
          <div style={{ flex: 1, display: "flex", alignItems: "center" }}>
            <Autocomplete
              style={{ margin: 8, width: 120 }}
              options={Array.from(
                new Set(deployment.replicas.map((e) => e.replica_id)),
              )}
              onInputChange={(_: any, value: string) => {
                changeFilter(
                  "replica_id",
                  value.trim() !== "-" ? value.trim() : "",
                );
              }}
              renderInput={(params: TextFieldProps) => (
                <TextField {...params} label="ID" />
              )}
            />
            <Autocomplete
              style={{ margin: 8, width: 120 }}
              options={Array.from(
                new Set(deployment.replicas.map((e) => e.state)),
              )}
              onInputChange={(_: any, value: string) => {
                changeFilter("state", value.trim());
              }}
              renderInput={(params: TextFieldProps) => (
                <TextField {...params} label="State" />
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
              count={Math.ceil(filteredReplicas.length / page.pageSize)}
              page={page.pageNo}
              onChange={(e, pageNo) => setPage("pageNo", pageNo)}
            />
          </div>
          <Table>
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
              {filteredReplicas
                .slice(
                  (page.pageNo - 1) * page.pageSize,
                  page.pageNo * page.pageSize,
                )
                .map((replica) => (
                  <ServeReplicaRow
                    key={replica.replica_id}
                    deployment={deployment}
                    replica={replica}
                  />
                ))}
            </TableBody>
          </Table>
        </TableContainer>
      </CollapsibleSection>
      <CollapsibleSection
        title="Logs"
        startExpanded
        className={classes.logSection}
      >
        <ServeEntityLogViewer deployments={[deployment]} />
      </CollapsibleSection>
    </div>
  );
};

export const ServeDeploymentDetailLayout = () => {
  const { applicationName, deploymentName } = useParams();

  const { application, deployment, loading, error } = useServeDeploymentDetails(
    applicationName,
    deploymentName,
  );

  if (loading) {
    return <Loading loading />;
  }

  if (error) {
    return <Typography color="error">{error.message}</Typography>;
  }

  if (!application) {
    return (
      <Typography color="error">
        Application with name "{applicationName}" not found.
      </Typography>
    );
  }

  if (!deployment) {
    return (
      <Typography color="error">
        Deployment with name "{deploymentName}" not found.
      </Typography>
    );
  }

  const appName = application.name ? application.name : "-";

  return (
    <React.Fragment>
      <MainNavPageInfo
        pageInfo={{
          id: "serveDeploymentDetail",
          title: deployment.name,
          pageTitle: `${deployment.name} | Serve Deployment`,
          path: `/serve/applications/${encodeURIComponent(
            appName,
          )}/${encodeURIComponent(deployment.name)}`,
        }}
      />
      <Outlet />
    </React.Fragment>
  );
};
