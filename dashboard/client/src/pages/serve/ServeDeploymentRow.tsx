import { TableCell, TableRow } from "@material-ui/core";
import React from "react";
import { CodeDialogButton } from "../../common/CodeDialogButton";
import { StatusChip } from "../../components/StatusChip";
import { ServeDeployment } from "../../type/serve";

export type ServeDeployentRowProps = {
  deployment: ServeDeployment;
};

export const ServeDeploymentRow = ({
  deployment: { name, deployment_status, message, deployment_config },
}: ServeDeployentRowProps) => {
  return (
    <TableRow>
      <TableCell align="center">{name}</TableCell>
      <TableCell align="center">
        <StatusChip type="serveDeployment" status={deployment_status} />
      </TableCell>
      <TableCell align="center">{message ? message : "-"}</TableCell>
      <TableCell align="center">
        {/* TODO(aguo): Add number of replicas (instead of target number of replicas) once available from API */}
        {deployment_config.num_replicas}
      </TableCell>
      <TableCell align="center">
        <CodeDialogButton
          title={`Deployment config for ${name}`}
          json={deployment_config}
        />
      </TableCell>
    </TableRow>
  );
};
