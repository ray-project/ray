import { styled } from "@mui/material/styles";
import React from "react";
import { RiCloseCircleFill, RiRecordCircleFill } from "react-icons/ri";
import { ServeDeployment } from "../type/serve";
import { JobRunningIcon } from "./JobStatus";
import { ClassNameProps } from "./props";

type ServeStatusIconProps = {
  deployment: ServeDeployment;
  small: boolean;
} & ClassNameProps;

const ServeSeccessElement = styled(RiRecordCircleFill)(({ theme }) => ({
  width: 20,
  height: 20,
  marginRight: 8,
  color: theme.palette.success.main,
}));

const ServeErrorElement = styled(RiCloseCircleFill)(({ theme }) => ({
  width: 20,
  height: 20,
  marginRight: 8,
  color: theme.palette.error.main,
}));

export const ServeStatusIcon = ({
  deployment,
  small,
  className,
}: ServeStatusIconProps) => {
  switch (deployment.status) {
    case "HEALTHY":
      return <ServeSeccessElement title="Healthy" />;
    case "UNHEALTHY":
      return <ServeErrorElement title="Unhealthy" />;
    default:
      // UPDATING
      return (
        <JobRunningIcon className={className} small={small} title="Updating" />
      );
  }
};
