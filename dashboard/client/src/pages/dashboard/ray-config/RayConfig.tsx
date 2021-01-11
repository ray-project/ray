import {
  createStyles,
  makeStyles,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  Theme,
  Typography,
} from "@material-ui/core";
import classNames from "classnames";
import React, { useCallback, useEffect, useRef } from "react";
import { useDispatch, useSelector } from "react-redux";
import { getRayConfig } from "../../../api";
import { StoreState } from "../../../store";
import { dashboardActions } from "../state";

const useRayConfigStyles = makeStyles((theme: Theme) =>
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
  }),
);
const { setRayConfig, setError } = dashboardActions;
const configSelector = (state: StoreState) => state.dashboard.rayConfig;

const RayConfig: React.FC = () => {
  const classes = useRayConfigStyles();
  const rayConfig = useSelector(configSelector);
  const dispatch = useDispatch();
  const refreshData = useCallback(async () => {
    try {
      const rayConfig = await getRayConfig();
      dispatch(setRayConfig(rayConfig));
    } catch (error) {
      dispatch(setError(error.toString()));
    }
  }, [dispatch]);
  const intervalId = useRef<any>(null);
  useEffect(() => {
    if (intervalId.current === null) {
      refreshData();
      intervalId.current = setInterval(refreshData, 10000);
    }
    const cleanup = () => {
      clearInterval(intervalId.current);
    };
    return cleanup;
  }, [refreshData]);
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
      value: rayConfig.autoscalingMode,
    },
    {
      key: "Head node type",
      value: rayConfig.headType,
    },
    {
      key: "Worker node type",
      value: rayConfig.workerType,
    },
    {
      key: "Min worker nodes",
      value: rayConfig.minWorkers,
    },
    {
      key: "Initial worker nodes",
      value: rayConfig.initialWorkers,
    },
    {
      key: "Max worker nodes",
      value: rayConfig.maxWorkers,
    },
    {
      key: "Idle timeout",
      value: `${rayConfig.idleTimeoutMinutes} ${
        rayConfig.idleTimeoutMinutes === 1 ? "minute" : "minutes"
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
};

export default RayConfig;
