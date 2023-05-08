import { TableCell, TableRow } from "@material-ui/core";
import React from "react";
import { Link as RouterLink } from "react-router-dom";
import {
  CodeDialogButton,
  CodeDialogButtonWithPreview,
} from "../../common/CodeDialogButton";
import { DurationText } from "../../common/DurationText";
import { formatDateFromTimeMs } from "../../common/formatUtils";
import { StatusChip } from "../../components/StatusChip";
import { ServeApplication } from "../../type/serve";

export type ServeApplicationRowProps = {
  application: ServeApplication;
};

export const ServeApplicationRow = ({
  application,
}: ServeApplicationRowProps) => {
  const {
    name,
    message,
    status,
    route_prefix,
    last_deployed_time_s,
    deployments,
    deployed_app_config,
  } = application;

  // TODO(aguo): Add duration and end time once available in the API
  return (
    <TableRow>
      <TableCell align="center">
        <RouterLink to={`applications/${name ? name : "-"}`}>
          {name ? name : "-"}
        </RouterLink>
      </TableCell>
      <TableCell align="center">{route_prefix}</TableCell>
      <TableCell align="center">
        <StatusChip type="serveApplication" status={status} />
      </TableCell>
      <TableCell align="center">
        {message ? (
          <CodeDialogButtonWithPreview title="Message details" code={message} />
        ) : (
          "-"
        )}
      </TableCell>
      <TableCell align="center">{Object.values(deployments).length}</TableCell>
      <TableCell align="center">
        {formatDateFromTimeMs(last_deployed_time_s * 1000)}
      </TableCell>
      <TableCell align="center">
        <DurationText startTime={last_deployed_time_s * 1000} />
      </TableCell>
      <TableCell align="center">
        {deployed_app_config ? (
          <CodeDialogButton
            title={
              name ? `Application config for ${name}` : `Application config`
            }
            code={deployed_app_config}
          />
        ) : (
          "-"
        )}
      </TableCell>
    </TableRow>
  );
};
