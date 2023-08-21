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
import {
  MultiTabLogViewer,
  MultiTabLogViewerTabDetails,
} from "../../common/MultiTabLogViewer";
import { Section } from "../../common/Section";
import Loading from "../../components/Loading";
import NewEventTable from "../../components/NewEventTable";

import { HelpInfo } from "../../components/Tooltip";
import { SeverityLevel } from "../../type/event";
import { ServeSystemActor } from "../../type/serve";
import { useFetchActor } from "../actor/hook/useActorDetail";
import { useServeApplications } from "./hook/useServeApplications";
import { ServeApplicationRow } from "./ServeApplicationRow";
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
    applicationsSection: {
      marginTop: theme.spacing(4),
    },
    section: {
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
    error,
    allServeApplications,
    page,
    setPage,
    httpProxies,
    changeFilter,
  } = useServeApplications();

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
            allApplications={allServeApplications}
            httpProxies={httpProxies}
            serveDetails={serveDetails}
          />
          <CollapsibleSection
            title="Applications"
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
          <CollapsibleSection
            title="Logs"
            startExpanded
            className={classes.section}
          >
            <Section noTopPadding>
              <ServeControllerLogs controller={serveDetails.controller_info} />
            </Section>
          </CollapsibleSection>
          <CollapsibleSection
            title="Event Table"
            startExpanded
            className={classes.section}
          >
            <NewEventTable
              defaultSeverityLevels={[
                SeverityLevel.WARNING,
                SeverityLevel.ERROR,
                SeverityLevel.INFO,
              ]}
              entityName="serve_app_name"
              entityId="*"
            />
          </CollapsibleSection>
        </React.Fragment>
      )}
      <ServeMetricsSection className={classes.section} />
    </div>
  );
};

type ServeControllerLogsProps = {
  controller: ServeSystemActor;
};

const ServeControllerLogs = ({
  controller: { actor_id, log_file_path },
}: ServeControllerLogsProps) => {
  const { data: fetchedActor } = useFetchActor(actor_id);

  if (!fetchedActor || !log_file_path) {
    return <Loading loading={true} />;
  }

  const tabs: MultiTabLogViewerTabDetails[] = [
    {
      title: "Controller logs",
      nodeId: fetchedActor.address.rayletId,
      filename: log_file_path.startsWith("/")
        ? log_file_path.substring(1)
        : log_file_path,
    },
    {
      title: "Other logs",
      contents:
        "Replica logs contain the application logs emitted by each Serve Replica.\n" +
        "To view replica logs, please click into a Serve application from " +
        "the table above to enter the Application details page.\nThen, click " +
        "into a Serve Replica in the Deployments table.\n\n" +
        "HTTP Proxy logs contains HTTP access logs for each HTTP Proxy.\n" +
        "To view HTTP Proxy logs, click into a HTTP Proxy from the Serve System " +
        "Details page.\nThis page can be accessed via the left tab menu or by " +
        'clicking "View system status and configuration" link above.',
    },
  ];
  return <MultiTabLogViewer tabs={tabs} />;
};
