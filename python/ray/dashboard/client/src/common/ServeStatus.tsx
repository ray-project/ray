import { Box, SxProps, Theme } from "@mui/material";
import React from "react";
import { RiCloseCircleFill, RiRecordCircleFill } from "react-icons/ri";
import { ServeDeployment } from "../type/serve";
import { JobRunningIcon } from "./JobStatus";
import { ClassNameProps } from "./props";

type ServeStatusIconProps = {
  deployment: ServeDeployment;
  small: boolean;
  sx?: SxProps<Theme>;
} & ClassNameProps;

export const ServeStatusIcon = ({
  deployment,
  small,
  className,
  sx,
}: ServeStatusIconProps) => {
  switch (deployment.status) {
    case "HEALTHY":
      return (
        <Box
          component={RiRecordCircleFill}
          sx={[
            {
              width: 20,
              height: 20,
              marginRight: 1,
              color: (theme) => theme.palette.success.main,
            },
            ...(Array.isArray(sx) ? sx : [sx]),
          ]}
          title="Healthy"
        />
      );
    case "UNHEALTHY":
      return (
        <Box
          component={RiCloseCircleFill}
          sx={[
            {
              width: 20,
              height: 20,
              marginRight: 1,
              color: (theme) => theme.palette.error.main,
            },
            ...(Array.isArray(sx) ? sx : [sx]),
          ]}
          title="Unhealthy"
        />
      );
    default:
      // UPDATING
      return (
        <JobRunningIcon
          className={className}
          sx={sx}
          small={small}
          title="Updating"
        />
      );
  }
};
