import { TableCell, TableRow } from "@material-ui/core";
import dayjs from "dayjs";
import React from "react";
import { Link as RouterLink } from "react-router-dom";
import { CodeDialogButton } from "../../common/CodeDialogButton";
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
    app_message,
    app_status,
    route_prefix,
    deployment_timestamp,
    deployments_details,
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
        <StatusChip type="serveApplication" status={app_status} />
      </TableCell>
      <TableCell align="center">{app_message ? app_message : "-"}</TableCell>
      <TableCell align="center">
        {Object.values(deployments_details).length}
      </TableCell>
      <TableCell align="center">
        {dayjs(Number(deployment_timestamp * 1000)).format(
          "YYYY/MM/DD HH:mm:ss",
        )}
      </TableCell>
      <TableCell align="center">
        <CodeDialogButton
          title={name ? `Application config for ${name}` : `Application config`}
          json={deployed_app_config}
        />
      </TableCell>
    </TableRow>
  );
};
