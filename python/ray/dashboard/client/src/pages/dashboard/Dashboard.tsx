import { Theme } from "@material-ui/core/styles/createMuiTheme";
import createStyles from "@material-ui/core/styles/createStyles";
import withStyles, { WithStyles } from "@material-ui/core/styles/withStyles";
import Table from "@material-ui/core/Table";
import TableBody from "@material-ui/core/TableBody";
import TableCell from "@material-ui/core/TableCell";
import TableHead from "@material-ui/core/TableHead";
import TableRow from "@material-ui/core/TableRow";
import Typography from "@material-ui/core/Typography";
import React from "react";
import { connect } from "react-redux";
import { getNodeInfo } from "../../api";
import { StoreState } from "../../store";
import NodeRowGroup from "./NodeRowGroup";
import { dashboardActions } from "./state";

const styles = (theme: Theme) =>
  createStyles({
    root: {
      backgroundColor: theme.palette.background.paper,
      padding: theme.spacing(2),
      "& > :not(:first-child)": {
        marginTop: theme.spacing(2)
      }
    },
    cell: {
      padding: theme.spacing(1),
      textAlign: "center",
      "&:last-child": {
        paddingRight: theme.spacing(1)
      }
    }
  });

const mapStateToProps = (state: StoreState) => ({
  nodeInfo: state.dashboard.nodeInfo,
  lastUpdatedAt: state.dashboard.lastUpdatedAt,
  error: state.dashboard.error
});

const mapDispatchToProps = dashboardActions;

class Dashboard extends React.Component<
  WithStyles<typeof styles> &
    ReturnType<typeof mapStateToProps> &
    typeof mapDispatchToProps
> {
  refresh = async () => {
    try {
      const nodeInfo = await getNodeInfo();
      this.props.setNodeInfo(nodeInfo);
    } catch (error) {
      this.props.setError(error.toString());
    } finally {
      setTimeout(this.refresh, 1000);
    }
  };

  async componentDidMount() {
    await this.refresh();
  }

  render() {
    const { classes, nodeInfo, lastUpdatedAt, error } = this.props;

    if (error !== null) {
      return (
        <Typography className={classes.root} color="error">
          {error}
        </Typography>
      );
    }

    if (nodeInfo === null) {
      return (
        <Typography className={classes.root} color="textSecondary">
          Loading...
        </Typography>
      );
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

    for (const client of nodeInfo.clients) {
      logCounts[client.ip] = { perWorker: {}, total: 0 };
      errorCounts[client.ip] = { perWorker: {}, total: 0 };
      for (const worker of client.workers) {
        logCounts[client.ip].perWorker[worker.pid] = 0;
        errorCounts[client.ip].perWorker[worker.pid] = 0;
      }
    }

    for (const ip of Object.keys(nodeInfo.log_counts)) {
      if (ip in logCounts) {
        for (const [pid, count] of Object.entries(nodeInfo.log_counts[ip])) {
          logCounts[ip].perWorker[pid] = count;
          logCounts[ip].total += count;
        }
      }
    }

    for (const ip of Object.keys(nodeInfo.error_counts)) {
      if (ip in errorCounts) {
        for (const [pid, count] of Object.entries(nodeInfo.error_counts[ip])) {
          errorCounts[ip].perWorker[pid] = count;
          errorCounts[ip].total += count;
        }
      }
    }

    return (
      <div className={classes.root}>
        <Typography variant="h5">Ray Dashboard</Typography>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell className={classes.cell} />
              <TableCell className={classes.cell}>Host</TableCell>
              <TableCell className={classes.cell}>Workers</TableCell>
              <TableCell className={classes.cell}>Uptime</TableCell>
              <TableCell className={classes.cell}>CPU</TableCell>
              <TableCell className={classes.cell}>RAM</TableCell>
              <TableCell className={classes.cell}>Disk</TableCell>
              {/*<TableCell className={classes.cell}>Sent</TableCell>*/}
              {/*<TableCell className={classes.cell}>Received</TableCell>*/}
              <TableCell className={classes.cell}>Logs</TableCell>
              <TableCell className={classes.cell}>Errors</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {nodeInfo.clients.map(client => (
              <NodeRowGroup
                key={client.ip}
                node={client}
                logCounts={logCounts[client.ip]}
                errorCounts={errorCounts[client.ip]}
              />
            ))}
          </TableBody>
        </Table>
        {lastUpdatedAt !== null && (
          <Typography align="center">
            Last updated: {new Date(lastUpdatedAt).toLocaleString()}
          </Typography>
        )}
      </div>
    );
  }
}

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(withStyles(styles)(Dashboard));
