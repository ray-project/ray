import { Box, SxProps, Theme, useTheme } from "@mui/material";
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

const useServeStatusIconStyles = (theme: Theme) => ({
  icon: (small: boolean) => ({
    width: small ? 16 : 20,
    height: small ? 16 : 20,
    marginRight: "8px",
  }),
  colorSuccess: {
    color: theme.palette.success.main,
  },
  colorError: {
    color: theme.palette.error.main,
  },
});

export const ServeStatusIcon = ({
  deployment,
  small,
  className,
  sx,
}: ServeStatusIconProps) => {
  const styles = useServeStatusIconStyles(useTheme());

  switch (deployment.status) {
    case "HEALTHY":
      return (
        <Box
          component={RiRecordCircleFill}
          sx={[
            styles.icon,
            styles.colorSuccess,
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
            styles.icon,
            styles.colorError,
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
