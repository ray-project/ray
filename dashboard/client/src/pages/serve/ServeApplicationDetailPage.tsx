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
import dayjs from "dayjs";
import React, { ReactElement } from "react";
import { useParams } from "react-router-dom";
import { CodeDialogButton } from "../../common/CodeDialogButton";
import { CollapsibleSection } from "../../common/CollapsibleSection";
import { DurationText } from "../../common/DurationText";
import { MetadataSection } from "../../components/MetadataSection";
import { StatusChip } from "../../components/StatusChip";
import { HelpInfo } from "../../components/Tooltip";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { useServeApplicationDetails } from "./hook/useServeApplications";
import { ServeDeploymentRow } from "./ServeDeploymentRow";

const useStyles = makeStyles((theme) =>
  createStyles({
    table: {
      tableLayout: "fixed",
    },
    helpInfo: {
      marginLeft: theme.spacing(1),
    },
  }),
);

const columns: { label: string; helpInfo?: ReactElement; width?: string }[] = [
  { label: "Name" },
  { label: "Status" },
  { label: "Message", width: "30%" },
  { label: "Replicas" },
  { label: "Deployment config" },
];

export const ServeApplicationDetailPage = () => {
  const classes = useStyles();
  const { name } = useParams();

  const {
    application,
    deployments,
    page,
    setPage,
    changeFilter,
    unfilteredList,
  } = useServeApplicationDetails(name);

  if (!application) {
    return (
      <Typography color="error">
        Application with name "{name}" not found.
      </Typography>
    );
  }

  const appName = application.name ? application.name : "-";
  return (
    <div>
      {/* Extra MainNavPageInfo to add an extra layer of nesting in breadcrumbs */}
      <MainNavPageInfo
        pageInfo={{
          id: "serveApplicationsList",
          title: "Applications",
        }}
      />
      <MainNavPageInfo
        pageInfo={{
          id: "serveApplicationDetail",
          title: appName,
          path: `/new/serve/applications/${appName}`,
        }}
      />
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
              <StatusChip type="serveApplication" status={application.status} />
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
              // TODO (aguo): Add number of replicas across all deployments once available in the UI
              value: "0",
            },
          },
          {
            label: "Application config",
            content: (
              <CodeDialogButton
                title={
                  application.name
                    ? `Application config for ${application.name}`
                    : `Application config`
                }
                code={application.deployed_app_config}
              />
            ),
          },
          {
            label: "Last deployed at",
            content: {
              value: dayjs(
                Number(application.last_deployed_time_s * 1000),
              ).format("YYYY/MM/DD HH:mm:ss"),
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
      <CollapsibleSection title="Deployments" startExpanded>
        <TableContainer>
          <div style={{ flex: 1, display: "flex", alignItems: "center" }}>
            <Autocomplete
              style={{ margin: 8, width: 120 }}
              options={Array.from(new Set(deployments.map((e) => e.name)))}
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
              count={Math.ceil(deployments.length / page.pageSize)}
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
              {deployments
                .slice(
                  (page.pageNo - 1) * page.pageSize,
                  page.pageNo * page.pageSize,
                )
                .map((deployment) => (
                  <ServeDeploymentRow
                    key={deployment.name}
                    deployment={deployment}
                  />
                ))}
            </TableBody>
          </Table>
        </TableContainer>
      </CollapsibleSection>
    </div>
  );
};
