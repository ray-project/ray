import HelpOutlineIcon from "@mui/icons-material/HelpOutline";
import { Tooltip } from "@mui/material";
import { styled } from "@mui/material/styles";
import React, { ReactNode } from "react";

export const StyledTooltip = styled(Tooltip)(({ theme }) => ({
  tooltip: {
    backgroundColor: theme.palette.background.paper,
    border: "1px solid #dadde9",
    color: theme.palette.text.primary,
    padding: theme.spacing(1),
  },
}));

const StyledHelpOutlineIcon = styled(HelpOutlineIcon)(({ theme }) => ({
  color: theme.palette.grey[500],
}));

type HelpInfoProps = {
  children: NonNullable<ReactNode>;
  className?: string;
};

export const HelpInfo = ({ children, className }: HelpInfoProps) => {
  return (
    <StyledTooltip className={className} title={children}>
      <StyledHelpOutlineIcon fontSize="small" />
    </StyledTooltip>
  );
};
