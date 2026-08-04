import ExpandLessIcon from "@mui/icons-material/ExpandLess";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import React from "react";

type MinimizerProps = {
  onClick: React.MouseEventHandler;
};

type ExpanderProps = {
  onClick: React.MouseEventHandler;
};

export const Minimizer: React.FC<MinimizerProps> = ({ onClick }) => (
  <ExpandLessIcon
    onClick={onClick}
    sx={(theme) => ({ color: theme.palette.text.secondary })}
  />
);

export const Expander: React.FC<ExpanderProps> = ({ onClick }) => (
  <ExpandMoreIcon
    onClick={onClick}
    sx={(theme) => ({ color: theme.palette.text.secondary })}
  />
);
