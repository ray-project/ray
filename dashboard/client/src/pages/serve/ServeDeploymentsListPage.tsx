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
import { styled } from "@mui/material/styles";
import React, { ReactElement } from "react";
import { CollapsibleSection } from "../../common/CollapsibleSection";
import { sliceToPage } from "../../common/util";
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

const RootDiv = styled("div")(({theme}) => ({
  padding: theme.spacing(3),
}));

const StyledTable = styled(Table)(({theme}) => ({
  tableLayout: "fixed",
}));

const ServeInstanceWarningAlert = styled(Alert)(({theme}) => ({
  marginBottom: theme.spacing(2),
}));

const StyledHelpInfo = styled(HelpInfo)(({theme}) => ({
  marginLeft: theme.spacing(1),
}));

const DeploymentsCollapsibleSection = styled(CollapsibleSection)(({theme}) => ({
  marginTop: theme.spacing(4),
}));

const StyledCollapsibleSection = styled(CollapsibleSection)(({theme}) => ({
  marginTop: theme.spacing(4),
}));

const StyledServeMetricsSection = styled(ServeMetricsSection)(({theme}) => ({
  marginTop: theme.spacing(4),
}));


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

  const {
    items: list,
    constrainedPage,
    maxPage,
  } = sliceToPage(serveApplications, page.pageNo, page.pageSize);

  return (
    <RootDiv>
      {serveDetails.http_options === undefined ? (
        <ServeInstanceWarningAlert severity="warning">
          Serve not started. Please deploy a serve application first.
        </ServeInstanceWarningAlert>
      ) : (
        <React.Fragment>
          <ServeSystemPreview
            allDeployments={serveDeployments}
            allApplications={serveApplications}
            proxies={proxies}
            serveDetails={serveDetails}
          />
          <DeploymentsCollapsibleSection
            title="Applications / Deployments"
            startExpanded
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
                count={maxPage}
                page={constrainedPage}
                onChange={(e, pageNo) => setPage("pageNo", pageNo)}
              />
              <StyledTable>
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
                            <StyledHelpInfo>
                              {helpInfo}
                            </StyledHelpInfo>
                          )}
                        </Box>
                      </TableCell>
                    ))}
                  </TableRow>
                </TableHead>
                <TableBody>
                  {list.map((application) => (
                    <ServeApplicationRows
                      key={`${application.name}`}
                      application={application}
                      startExpanded
                    />
                  ))}
                </TableBody>
              </StyledTable>
            </TableContainer>
          </DeploymentsCollapsibleSection>
          <StyledCollapsibleSection
            title="Logs"
            startExpanded
          >
            <ServeEntityLogViewer
              controller={serveDetails.controller_info}
              proxies={proxies}
              deployments={serveDeployments}
            />
          </StyledCollapsibleSection>
        </React.Fragment>
      )}
      <StyledServeMetricsSection
        metricsConfig={APPS_METRICS_CONFIG}
      />
    </RootDiv>
  );
};
