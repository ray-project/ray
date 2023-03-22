import { TableCell, TableRow } from "@material-ui/core";
import React from "react";
import {
  CodeDialogButton,
  CodeDialogButtonWithPreview,
} from "../../common/CodeDialogButton";
import { StatusChip } from "../../components/StatusChip";
import { ServeDeployment } from "../../type/serve";

export type ServeDeployentRowProps = {
  deployment: ServeDeployment;
};

export const ServeDeploymentRow = ({
  deployment: { name, status, message, deployment_config },
}: ServeDeployentRowProps) => {
  return (
    <TableRow>
      <TableCell align="center">{name}</TableCell>
      <TableCell align="center">
        <StatusChip type="serveDeployment" status={status} />
      </TableCell>
      <TableCell align="center">
        {message ? (
          <CodeDialogButtonWithPreview title="Message details" code={message} />
        ) : (
          "-"
        )}
      </TableCell>
      <TableCell align="center">
        {/* TODO(aguo): Add number of replicas (instead of target number of replicas) once available from API */}
        {deployment_config.num_replicas}
      </TableCell>
      <TableCell align="center">
        <CodeDialogButton
          title={`Deployment config for ${name}`}
          code={deployment_config}
        />
      </TableCell>
    </TableRow>
  );
};
