import { createStyles, makeStyles } from "@material-ui/core";
import classNames from "classnames";
import React from "react";
import {
  RiCheckboxCircleFill,
  RiCloseCircleFill,
  RiLoader4Line,
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
    case "NOT_STARTED":
      return (
        <RiCheckboxCircleFill
          className={classNames(classes.icon, classes.colorSuccess)}
        />
      );
    case "RUNNING":
    case "DEPLOY_FAILED":
      return (
        <RiCloseCircleFill
          className={classNames(classes.icon, classes.colorError)}
        />
      );
    default:
      return <JobRunningIcon className={className} small={small} />;
  }
};
