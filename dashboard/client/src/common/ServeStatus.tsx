import { createStyles, makeStyles } from "@material-ui/core";
import classNames from "classnames";
import React from "react";
import {
  RiCloseCircleFill,
  RiRecordCircleFill,
  RiStopCircleFill,
} from "react-icons/ri";
import { ServeApplication } from "../type/serve";
import { JobRunningIcon } from "./JobStatus";
import { ClassNameProps } from "./props";

type ServeStatusIconProps = {
  app: ServeApplication;
  small: boolean;
} & ClassNameProps;

const useServeStatusIconStyles = makeStyles((theme) =>
  createStyles({
    icon: {
      width: 20,
      height: 20,
      marginRight: 8,
    },
    iconSmall: {
      width: 16,
      height: 16,
    },
    colorSuccess: {
      color: theme.palette.success.main,
    },
    colorError: {
      color: theme.palette.error.main,
    },
  }),
);

export const ServeStatusIcon = ({
  app,
  small,
  className,
}: ServeStatusIconProps) => {
  const classes = useServeStatusIconStyles();

  switch (app.status) {
    case "RUNNING":
      return (
        <RiRecordCircleFill
          data-testid="serve-status-icon"
          className={classNames(classes.icon, classes.colorSuccess)}
        />
      );
    case "NOT_STARTED":
      return (
        <RiStopCircleFill
          data-testid="serve-status-icon"
          className={classes.icon}
        />
      );
    case "DEPLOY_FAILED":
      return (
        <RiCloseCircleFill
          data-testid="serve-status-icon"
          className={classNames(classes.icon, classes.colorError)}
        />
      );
    default:
      // DEPLOYING || DELETEING
      return (
        <JobRunningIcon
          data-testid="serve-status-icon"
          className={className}
          small={small}
        />
      );
  }
};
