import {
  Autocomplete,
  Box,
  Pagination,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  TextFieldProps,
  Typography,
} from "@mui/material";
import React, { ReactElement } from "react";
import { Outlet, useParams } from "react-router-dom";
import { CodeDialogButton } from "../../common/CodeDialogButton";
import { CollapsibleSection } from "../../common/CollapsibleSection";
import { DurationText } from "../../common/DurationText";
import { formatDateFromTimeMs } from "../../common/formatUtils";
import { sliceToPage } from "../../common/util";
import Loading from "../../components/Loading";
import { MetadataSection } from "../../components/MetadataSection";
import { StatusChip } from "../../components/StatusChip";
import { HelpInfo } from "../../components/Tooltip";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { useServeApplicationDetails } from "./hook/useServeApplications";
import { ServeDeploymentRow } from "./ServeDeploymentRow";

const columns: { label: string; helpInfo?: ReactElement; width?: string }[] = [
  { label: "Deployment name" },
  { label: "Status" },
  { label: "Status message", width: "30%" },
  { label: "Num replicas" },
  { label: "Actions" },
  { label: "Route prefix" },
  { label: "Last deployed at" },
  { label: "Duration (since last deploy)" },
];

export const ServeApplicationDetailPage = () => {
  const { applicationName } = useParams();

  const {
    application,
    filteredDeployments,
    page,
    setPage,
    changeFilter,
    allDeployments,
  } = useServeApplicationDetails(applicationName);

  if (!application) {
    return (
      <Typography color="error">
        Application with name "{applicationName}" not found.
      </Typography>
    );
  }

  const appName = application.name ? application.name : "-";

  const {
    items: list,
    constrainedPage,
    maxPage,
  } = sliceToPage(filteredDeployments, page.pageNo, page.pageSize);

  return (
    <Box sx={{ padding: 3 }}>
      <MetadataSection
        metadataList={[
          {
            label: "Name",
            content: {
              value: appName,
            },
          },
          {
            label: "Route prefix",
            content: {
              value: application.route_prefix,
            },
          },
          {
            label: "Status",
            content: (
              <React.Fragment>
                <StatusChip
                  type="serveApplication"
                  status={application.status}
                />{" "}
                {application.message && (
                  <CodeDialogButton
                    title="Status details"
                    code={application.message}
                    buttonText="View details"
                  />
                )}
              </React.Fragment>
            ),
          },
          {
            label: "Deployments",
            content: {
              value: `${Object.keys(application.deployments).length}`,
            },
          },
          {
            label: "Replicas",
            content: {
              value: Object.values(application.deployments)
                .map(({ replicas }) => replicas.length)
                .reduce((acc, curr) => acc + curr, 0)
                .toString(),
            },
          },
          {
            label: "Application config",
            content: application.deployed_app_config ? (
              <CodeDialogButton
                title={
                  application.name
                    ? `Application config for ${application.name}`
                    : `Application config`
                }
                code={application.deployed_app_config}
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
          {
            label: "Import path",
            content: {
              value: application?.deployed_app_config?.import_path || "-",
            },
          },
        ]}
      />
      <CollapsibleSection title="Deployments" startExpanded>
        <TableContainer>
          <div style={{ flex: 1, display: "flex", alignItems: "center" }}>
            <Autocomplete
              style={{ margin: 8, width: 120 }}
              options={Array.from(new Set(allDeployments.map((e) => e.name)))}
              onInputChange={(_: any, value: string) => {
                changeFilter("name", value.trim() !== "-" ? value.trim() : "");
              }}
              renderInput={(params: TextFieldProps) => (
                <TextField {...params} label="Name" />
              )}
            />
            <Autocomplete
              style={{ margin: 8, width: 120 }}
              options={Array.from(new Set(allDeployments.map((e) => e.status)))}
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
              }}
            />
          </div>
          <div style={{ display: "flex", alignItems: "center" }}>
            <Pagination
              count={maxPage}
              page={constrainedPage}
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
                        <HelpInfo sx={{ marginLeft: 1 }}>{helpInfo}</HelpInfo>
                      )}
                    </Box>
                  </TableCell>
                ))}
              </TableRow>
            </TableHead>
            <TableBody>
              {list.map((deployment) => (
                <ServeDeploymentRow
                  key={deployment.name}
                  deployment={deployment}
                  application={application}
                  showExpandColumn={false}
                />
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      </CollapsibleSection>
    </Box>
  );
};

export const ServeApplicationDetailLayout = () => {
  const { applicationName } = useParams();

  const { loading, application } = useServeApplicationDetails(applicationName);

  if (loading) {
    return <Loading loading />;
  } else if (!application) {
    return (
      <Typography color="error">
        Application with name "{applicationName}" not found.
      </Typography>
    );
  }

  const appName = application.name ? application.name : "-";

  return (
    <React.Fragment>
      <MainNavPageInfo
        pageInfo={{
          id: "serveApplicationDetail",
          title: appName,
          pageTitle: `${appName} | Serve Application`,
          path: `/serve/applications/${encodeURIComponent(appName)}`,
        }}
      />
      <Outlet />
    </React.Fragment>
  );
};
