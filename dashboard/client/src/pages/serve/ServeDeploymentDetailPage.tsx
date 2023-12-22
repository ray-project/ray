import {
  Box,
  createStyles,
  makeStyles,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from "@material-ui/core";
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

  const { application, deployment } = useServeDeploymentDetails(
    applicationName,
    deploymentName,
  );

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
              {deployment.replicas.map((replica) => (
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
