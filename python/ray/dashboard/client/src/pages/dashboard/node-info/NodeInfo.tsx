import {
  createStyles,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  Theme,
  Typography,
  withStyles,
  WithStyles,
} from "@material-ui/core";
import React from "react";
import { connect } from "react-redux";
import { RayletInfoResponse } from "../../../api";
import { StoreState } from "../../../store";
import Errors from "./dialogs/errors/Errors";
import Logs from "./dialogs/logs/Logs";
import NodeRowGroup from "./NodeRowGroup";
import TotalRow from "./TotalRow";

const clusterWorkerPids = (
  rayletInfo: RayletInfoResponse,
): Map<string, Set<string>> => {
  // Groups PIDs registered with the raylet by node IP address
  // This is used to filter out processes belonging to other ray clusters.
  const nodeMap = new Map();
  const workerPids = new Set();
  for (const [nodeIp, { workersStats }] of Object.entries(rayletInfo.nodes)) {
    for (const worker of workersStats) {
      if (!worker.isDriver) {
        workerPids.add(worker.pid.toString());
      }
    }
    nodeMap.set(nodeIp, workerPids);
  }
  return nodeMap;
};

const styles = (theme: Theme) =>
  createStyles({
    table: {
      marginTop: theme.spacing(1),
    },
    cell: {
      padding: theme.spacing(1),
      textAlign: "center",
      "&:last-child": {
        paddingRight: theme.spacing(1),
      },
    },
  });

const mapStateToProps = (state: StoreState) => ({
  nodeInfo: state.dashboard.nodeInfo,
  rayletInfo: state.dashboard.rayletInfo,
});

type State = {
  logDialog: { hostname: string; pid: number | null } | null;
  errorDialog: { hostname: string; pid: number | null } | null;
};

class NodeInfo extends React.Component<
  WithStyles<typeof styles> & ReturnType<typeof mapStateToProps>
> {
  state: State = {
    logDialog: null,
    errorDialog: null,
  };

  setLogDialog = (hostname: string, pid: number | null) => {
    this.setState({ logDialog: { hostname, pid } });
  };

  clearLogDialog = () => {
    this.setState({ logDialog: null });
  };

  setErrorDialog = (hostname: string, pid: number | null) => {
    this.setState({ errorDialog: { hostname, pid } });
  };

  clearErrorDialog = () => {
    this.setState({ errorDialog: null });
  };

  render() {
    const { classes, nodeInfo, rayletInfo } = this.props;
    const { logDialog, errorDialog } = this.state;

    if (nodeInfo === null || rayletInfo === null) {
      return <Typography color="textSecondary">Loading...</Typography>;
    }

    const logCounts: {
      [ip: string]: {
        perWorker: {
          [pid: string]: number;
        };
        total: number;
      };
    } = {};

    const errorCounts: {
      [ip: string]: {
        perWorker: {
          [pid: string]: number;
        };
        total: number;
      };
    } = {};

    // We fetch data about which process IDs are registered with
    // the cluster's raylet for each node. We use this to filter
    // the worker data contained in the node info data because
    // the node info can contain data from more than one cluster
    // if more than one cluster is running on a machine.
    const clusterWorkerPidsByIp = clusterWorkerPids(rayletInfo);
    const clusterTotalWorkers = Array.from(
      clusterWorkerPidsByIp.values(),
    ).reduce((acc, workerSet) => acc + workerSet.size, 0);
    // Initialize inner structure of the count objects
    for (const client of nodeInfo.clients) {
      const clusterWorkerPids = clusterWorkerPidsByIp.get(client.ip);
      if (!clusterWorkerPids) {
        continue;
      }
      const filteredLogEntries = Object.entries(
        nodeInfo.log_counts[client.ip] || {},
      ).filter(([pid, _]) => clusterWorkerPids.has(pid));
      const totalLogEntries = filteredLogEntries.reduce(
        (acc, [_, count]) => acc + count,
        0,
      );
      logCounts[client.ip] = {
        perWorker: Object.fromEntries(filteredLogEntries),
        total: totalLogEntries,
      };

      const filteredErrEntries = Object.entries(
        nodeInfo.error_counts[client.ip] || {},
      ).filter(([pid, _]) => clusterWorkerPids.has(pid));
      const totalErrEntries = filteredErrEntries.reduce(
        (acc, [_, count]) => acc + count,
        0,
      );
      errorCounts[client.ip] = {
        perWorker: Object.fromEntries(filteredErrEntries),
        total: totalErrEntries,
      };
    }

    return (
      <React.Fragment>
        <Table className={classes.table}>
          <TableHead>
            <TableRow>
              <TableCell className={classes.cell} />
              <TableCell className={classes.cell}>Host</TableCell>
              <TableCell className={classes.cell}>Workers</TableCell>
              <TableCell className={classes.cell}>Uptime</TableCell>
              <TableCell className={classes.cell}>CPU</TableCell>
              <TableCell className={classes.cell}>RAM</TableCell>
              <TableCell className={classes.cell}>Disk</TableCell>
              <TableCell className={classes.cell}>Sent</TableCell>
              <TableCell className={classes.cell}>Received</TableCell>
              <TableCell className={classes.cell}>Logs</TableCell>
              <TableCell className={classes.cell}>Errors</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {nodeInfo.clients.map((client) => {
              const clusterWorkerPids =
                clusterWorkerPidsByIp.get(client.ip) || new Set();
              return (
                <NodeRowGroup
                  key={client.ip}
                  clusterWorkers={client.workers
                    .filter((worker) =>
                      clusterWorkerPids.has(worker.pid.toString()),
                    )
                    .sort((w1, w2) => {
                      if (w2.cmdline[0] === "ray::IDLE") {
                        return -1;
                      }
                      if (w1.cmdline[0] === "ray::IDLE") {
                        return 1;
                      }
                      return w1.pid < w2.pid ? -1 : 1;
                    })}
                  node={client}
                  raylet={
                    client.ip in rayletInfo.nodes
                      ? rayletInfo.nodes[client.ip]
                      : null
                  }
                  logCounts={logCounts[client.ip]}
                  errorCounts={errorCounts[client.ip]}
                  setLogDialog={this.setLogDialog}
                  setErrorDialog={this.setErrorDialog}
                  initialExpanded={nodeInfo.clients.length <= 1}
                />
              );
            })}
            <TotalRow
              clusterTotalWorkers={clusterTotalWorkers}
              nodes={nodeInfo.clients}
              logCounts={logCounts}
              errorCounts={errorCounts}
            />
          </TableBody>
        </Table>
        {logDialog !== null && (
          <Logs
            clearLogDialog={this.clearLogDialog}
            hostname={logDialog.hostname}
            pid={logDialog.pid}
          />
        )}
        {errorDialog !== null && (
          <Errors
            clearErrorDialog={this.clearErrorDialog}
            hostname={errorDialog.hostname}
            pid={errorDialog.pid}
          />
        )}
      </React.Fragment>
    );
  }
}

export default connect(mapStateToProps)(withStyles(styles)(NodeInfo));
