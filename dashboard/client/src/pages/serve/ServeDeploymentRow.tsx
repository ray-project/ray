import {
  createStyles,
  Link,
  makeStyles,
  TableCell,
  TableRow,
  Typography,
} from "@material-ui/core";
import React, { useState } from "react";
import DialogWithTitle from "../../common/DialogWithTitle";
import { StatusChip } from "../../components/StatusChip";
import { ServeDeployment } from "../../type/serve";

const useStyles = makeStyles((theme) =>
  createStyles({
    configText: {
      whiteSpace: "pre",
    },
  }),
);

export type ServeDeployentRowProps = {
  deployment: ServeDeployment;
};

export const ServeDeploymentRow = ({
  deployment: { name, deployment_status, message, deployment_config },
}: ServeDeployentRowProps) => {
  const classes = useStyles();

  const [showConfigDialog, setShowConfigDialog] = useState(false);

  const handleConfigClick = () => {
    setShowConfigDialog(true);
  };

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
        -{/* TODO(aguo): Add start time once available from API */}
      </TableCell>
      <TableCell align="center">
        -{/* TODO(aguo): Add end time once available from API */}
      </TableCell>
      <TableCell align="center">
        -{/* TODO(aguo): Add duration once available from API */}
      </TableCell>
      <TableCell align="center">
        <Link component="button" onClick={handleConfigClick}>
          See config
        </Link>
        {showConfigDialog && (
          <DialogWithTitle
            title={`Deployment config for ${name}`}
            handleClose={() => {
              setShowConfigDialog(false);
            }}
          >
            <Typography className={classes.configText}>
              {JSON.stringify(deployment_config, undefined, 2)}
            </Typography>
          </DialogWithTitle>
        )}
      </TableCell>
    </TableRow>
  );
};
