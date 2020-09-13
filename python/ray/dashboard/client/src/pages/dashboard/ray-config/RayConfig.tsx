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
import classNames from "classnames";
import React from "react";
import { connect } from "react-redux";
import { getRayConfig } from "../../../api";
import { StoreState } from "../../../store";
import { dashboardActions } from "../state";

const styles = (theme: Theme) =>
  createStyles({
    table: {
      marginTop: theme.spacing(1),
      width: "auto",
    },
    cell: {
      paddingTop: theme.spacing(1),
      paddingBottom: theme.spacing(1),
      paddingLeft: theme.spacing(3),
      paddingRight: theme.spacing(3),
      textAlign: "center",
      "&:last-child": {
        paddingRight: theme.spacing(3),
      },
    },
    key: {
      color: theme.palette.text.secondary,
    },
  });

const mapStateToProps = (state: StoreState) => ({
  rayConfig: state.dashboard.rayConfig,
});

const mapDispatchToProps = dashboardActions;

class RayConfig extends React.Component<
  WithStyles<typeof styles> &
    ReturnType<typeof mapStateToProps> &
    typeof mapDispatchToProps
> {
  refreshRayConfig = async () => {
    try {
      const rayConfig = await getRayConfig();
      this.props.setRayConfig(rayConfig);
    } catch (error) {
    } finally {
      setTimeout(this.refreshRayConfig, 10 * 1000);
    }
  };

  async componentDidMount() {
    await this.refreshRayConfig();
  }

  render() {
    const { classes, rayConfig } = this.props;

    if (rayConfig === null) {
      return (
        <Typography color="textSecondary">
          No Ray configuration detected.
        </Typography>
      );
    }

    const formattedRayConfig = [
      {
        key: "Autoscaling mode",
        value: rayConfig.autoscaling_mode,
      },
      {
        key: "Head node type",
        value: rayConfig.head_type,
      },
      {
        key: "Worker node type",
        value: rayConfig.worker_type,
      },
      {
        key: "Min worker nodes",
        value: rayConfig.min_workers,
      },
      {
        key: "Initial worker nodes",
        value: rayConfig.initial_workers,
      },
      {
        key: "Max worker nodes",
        value: rayConfig.max_workers,
      },
      {
        key: "Idle timeout",
        value: `${rayConfig.idle_timeout_minutes} ${
          rayConfig.idle_timeout_minutes === 1 ? "minute" : "minutes"
        }`,
      },
    ];

    return (
      <div>
        <Typography>Ray cluster configuration:</Typography>
        <Table className={classes.table}>
          <TableHead>
            <TableRow>
              <TableCell className={classes.cell}>Setting</TableCell>
              <TableCell className={classes.cell}>Value</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {formattedRayConfig.map(({ key, value }, index) => (
              <TableRow key={index}>
                <TableCell className={classNames(classes.cell, classes.key)}>
                  {key}
                </TableCell>
                <TableCell className={classes.cell}>{value}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
    );
  }
}

export default connect(
  mapStateToProps,
  mapDispatchToProps,
)(withStyles(styles)(RayConfig));
