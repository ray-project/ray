import createStyles from "@mui/styles/createStyles";
import makeStyles from "@mui/styles/makeStyles";
import classNames from "classnames";
import React from "react";
import { RiCloseCircleFill, RiRecordCircleFill } from "react-icons/ri";
import { ServeDeployment } from "../type/serve";
import { JobRunningIcon } from "./JobStatus";
import { ClassNameProps } from "./props";

type ServeStatusIconProps = {
  deployment: ServeDeployment;
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
  deployment,
  small,
  className,
}: ServeStatusIconProps) => {
  const classes = useServeStatusIconStyles();

  switch (deployment.status) {
    case "HEALTHY":
      return (
        <RiRecordCircleFill
          className={classNames(classes.icon, classes.colorSuccess)}
          title="Healthy"
        />
      );
    case "UNHEALTHY":
      return (
        <RiCloseCircleFill
          className={classNames(classes.icon, classes.colorError)}
          title="Unhealthy"
        />
      );
    default:
      // UPDATING
      return (
        <JobRunningIcon className={className} small={small} title="Updating" />
      );
  }
};
