import { Box, IconButton, Link, TableCell, TableRow } from "@mui/material";
import React, { useState } from "react";
import { RiArrowDownSLine, RiArrowRightSLine } from "react-icons/ri";
import { Link as RouterLink } from "react-router-dom";
import {
  CodeDialogButton,
  CodeDialogButtonWithPreview,
} from "../../common/CodeDialogButton";
import { DurationText } from "../../common/DurationText";
import { formatDateFromTimeMs } from "../../common/formatUtils";
import { StatusChip } from "../../components/StatusChip";
import { ServeApplication } from "../../type/serve";
import { ServeDeploymentRow } from "./ServeDeploymentRow";

export type ServeApplicationRowsProps = {
  application: ServeApplication;
  startExpanded?: boolean;
};

export const ServeApplicationRows = ({
  application,
  startExpanded = true,
}: ServeApplicationRowsProps) => {
  const [isExpanded, setExpanded] = useState(startExpanded);

  const {
    name,
    message,
    status,
    route_prefix,
    last_deployed_time_s,
    deployments,
    deployed_app_config,
  } = application;

  const deploymentsList = Object.values(deployments);

  const onExpandButtonClick = () => {
    setExpanded(!isExpanded);
  };

  // TODO(aguo): Add duration and end time once available in the API
  return (
    <React.Fragment>
      <TableRow>
        <TableCell>
          <IconButton size="small" onClick={onExpandButtonClick}>
            {!isExpanded ? (
              <Box
                component={RiArrowRightSLine}
                sx={{
                  color: (theme) => theme.palette.text.secondary,
                  fontSize: "1.5em",
                  verticalAlign: "middle",
                }}
              />
            ) : (
              <Box
                component={RiArrowDownSLine}
                sx={{
                  color: (theme) => theme.palette.text.secondary,
                  fontSize: "1.5em",
                  verticalAlign: "middle",
                }}
              />
            )}
          </IconButton>
        </TableCell>
        <TableCell align="center" sx={{ fontWeight: 500 }}>
          <Link
            component={RouterLink}
            to={`applications/${name ? encodeURIComponent(name) : "-"}`}
          >
            {name ? name : "-"}
          </Link>
        </TableCell>
        <TableCell align="center">
          <StatusChip type="serveApplication" status={status} />
        </TableCell>
        <TableCell align="center">
          {message ? (
            <CodeDialogButtonWithPreview
              title="Message details"
              code={message}
            />
          ) : (
            "-"
          )}
        </TableCell>
        <TableCell align="center">
          {/* placeholder for num_replicas, which does not apply to an application */}
          -
        </TableCell>
        <TableCell align="center">
          {deployed_app_config ? (
            <CodeDialogButton
              title={
                name ? `Application config for ${name}` : `Application config`
              }
              code={deployed_app_config}
              buttonText="View config"
            />
          ) : (
            "-"
          )}
        </TableCell>
        <TableCell align="center">{route_prefix}</TableCell>
        <TableCell align="center">
          {formatDateFromTimeMs(last_deployed_time_s * 1000)}
        </TableCell>
        <TableCell align="center">
          <DurationText startTime={last_deployed_time_s * 1000} />
        </TableCell>
      </TableRow>
      {isExpanded &&
        deploymentsList.map((deployment) => (
          <ServeDeploymentRow
            key={`${application.name}-${deployment.name}`}
            deployment={deployment}
            application={application}
            showExpandColumn
          />
        ))}
    </React.Fragment>
  );
};
