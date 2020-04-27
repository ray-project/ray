import {
  createStyles,
  TableCell,
  TableRow,
  Theme,
  WithStyles,
  withStyles,
} from "@material-ui/core";
import LayersIcon from "@material-ui/icons/Layers";
import React from "react";
import { NodeInfoResponse } from "../../../api";
import { ClusterCPU } from "./features/CPU";
import { ClusterDisk } from "./features/Disk";
import { makeClusterErrors } from "./features/Errors";
import { ClusterHost } from "./features/Host";
import { makeClusterLogs } from "./features/Logs";
import { ClusterRAM } from "./features/RAM";
import { ClusterReceived } from "./features/Received";
import { ClusterSent } from "./features/Sent";
import { ClusterUptime } from "./features/Uptime";
import { ClusterWorkers } from "./features/Workers";

const styles = (theme: Theme) =>
  createStyles({
    cell: {
      borderTopColor: theme.palette.divider,
      borderTopStyle: "solid",
      borderTopWidth: 2,
      padding: theme.spacing(1),
      textAlign: "center",
      "&:last-child": {
        paddingRight: theme.spacing(1),
      },
    },
    totalIcon: {
      color: theme.palette.text.secondary,
      fontSize: "1.5em",
      verticalAlign: "middle",
    },
  });

type Props = {
  nodes: NodeInfoResponse["clients"];
  logCounts: {
    [ip: string]: {
      perWorker: { [pid: string]: number };
      total: number;
    };
  };
  errorCounts: {
    [ip: string]: {
      perWorker: { [pid: string]: number };
      total: number;
    };
  };
};

class TotalRow extends React.Component<Props & WithStyles<typeof styles>> {
  render() {
    const { classes, nodes, logCounts, errorCounts } = this.props;

    const features = [
      { ClusterFeature: ClusterHost },
      { ClusterFeature: ClusterWorkers },
      { ClusterFeature: ClusterUptime },
      { ClusterFeature: ClusterCPU },
      { ClusterFeature: ClusterRAM },
      { ClusterFeature: ClusterDisk },
      { ClusterFeature: ClusterSent },
      { ClusterFeature: ClusterReceived },
      { ClusterFeature: makeClusterLogs(logCounts) },
      { ClusterFeature: makeClusterErrors(errorCounts) },
    ];

    return (
      <TableRow hover>
        <TableCell className={classes.cell}>
          <LayersIcon className={classes.totalIcon} />
        </TableCell>
        {features.map(({ ClusterFeature }, index) => (
          <TableCell className={classes.cell} key={index}>
            <ClusterFeature nodes={nodes} />
          </TableCell>
        ))}
      </TableRow>
    );
  }
}

export default withStyles(styles)(TotalRow);
