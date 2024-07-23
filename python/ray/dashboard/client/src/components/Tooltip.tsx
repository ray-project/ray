import HelpOutlineIcon from "@mui/icons-material/HelpOutline";
import { SxProps, Theme, Tooltip, TooltipProps } from "@mui/material";
import React, { ReactNode } from "react";

export const StyledTooltip = (props: TooltipProps) => {
  return (
    <Tooltip
      componentsProps={{
        tooltip: {
          sx: (theme) => ({
            backgroundColor: theme.palette.background.paper,
            border: "1px solid #dadde9",
            color: theme.palette.text.primary,
            padding: 1,
          }),
        },
      }}
      {...props}
    />
  );
};

type HelpInfoProps = {
  children: NonNullable<ReactNode>;
  className?: string;
  sx?: SxProps<Theme>;
};

export const HelpInfo = ({ children, className, sx }: HelpInfoProps) => {
  return (
    <StyledTooltip className={className} title={children}>
      <HelpOutlineIcon
        fontSize="small"
        sx={[
          { color: (theme) => theme.palette.grey[500] },
          ...(Array.isArray(sx) ? sx : [sx]),
        ]}
      />
    </StyledTooltip>
  );
};
