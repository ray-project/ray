import ExpandLessIcon from "@material-ui/icons/ExpandLess";
import ExpandMoreIcon from "@material-ui/icons/ExpandMore";
import React from "react";

type MinimizerProps = {
  onClick: React.MouseEventHandler;
};

type ExpanderProps = {
  onClick: React.MouseEventHandler;
};

export const Minimizer: React.FC<MinimizerProps> = ({ onClick }) => (
  <ExpandLessIcon onClick={onClick} />
);

export const Expander: React.FC<ExpanderProps> = ({ onClick }) => (
  <ExpandMoreIcon onClick={onClick} />
);
