import HelpOutlineIcon from "@mui/icons-material/HelpOutline";
import { SxProps, Theme, Tooltip, TooltipProps, useTheme } from "@mui/material";
import React, { ReactNode } from "react";

const useStyles = (theme: Theme) => ({
  root: {
    backgroundColor: theme.palette.background.paper,
    border: "1px solid #dadde9",
    color: theme.palette.text.primary,
    padding: theme.spacing(1),
  },
});

export const StyledTooltip = (props: TooltipProps) => {
  const styles = useStyles(useTheme());

  return (
    <Tooltip componentsProps={{ tooltip: { sx: styles.root } }} {...props} />
  );
};

const useHelpInfoStyles = (theme: Theme) => ({
  helpIcon: {
    color: theme.palette.grey[500],
  },
});

type HelpInfoProps = {
  children: NonNullable<ReactNode>;
  className?: string;
  sx?: SxProps<Theme>;
};

export const HelpInfo = ({ children, className, sx }: HelpInfoProps) => {
  const styles = useHelpInfoStyles(useTheme());

  return (
    <StyledTooltip className={className} title={children}>
      <HelpOutlineIcon
        fontSize="small"
        sx={[styles.helpIcon, ...(Array.isArray(sx) ? sx : [sx])]}
      />
    </StyledTooltip>
  );
};
