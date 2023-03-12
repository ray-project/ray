import {
  createStyles,
  Link,
  makeStyles,
  TableCell,
  TableRow,
  Typography,
} from "@material-ui/core";
import dayjs from "dayjs";
import React, { useState } from "react";
import { Link as RouterLink } from "react-router-dom";
import DialogWithTitle from "../../common/DialogWithTitle";
import { StatusChip } from "../../components/StatusChip";
import { ServeApplication } from "../../type/serve";

const useStyles = makeStyles((theme) =>
  createStyles({
    configText: {
      whiteSpace: "pre",
    },
  }),
);

export type ServeApplicationRowProps = {
  serveApplication: ServeApplication;
};

export const ServeApplicationRow = ({
  serveApplication: {
    name,
    app_message,
    app_status,
    deployed_app_config,
    route_prefix,
    deployment_timestamp,
    deployments_details,
    docs_path,
  },
}: ServeApplicationRowProps) => {
  const classes = useStyles();

  const [showConfigDialog, setShowConfigDialog] = useState(false);

  const handleConfigClick = () => {
    setShowConfigDialog(true);
  };

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
        <Link component="button" onClick={handleConfigClick}>
          See config
        </Link>
        {showConfigDialog && (
          <DialogWithTitle
            title={
              name ? `Application config for ${name}` : `Application config`
            }
            handleClose={() => {
              setShowConfigDialog(false);
            }}
          >
            <Typography className={classes.configText}>
              {JSON.stringify(deployed_app_config, undefined, 2)}
            </Typography>
          </DialogWithTitle>
        )}
      </TableCell>
    </TableRow>
  );
};
