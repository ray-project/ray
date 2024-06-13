import HelpOutlineIcon from "@mui/icons-material/HelpOutline";
import { Tooltip, TooltipProps } from "@mui/material";
import createStyles from "@mui/styles/createStyles";
import makeStyles from "@mui/styles/makeStyles";
import React, { ReactNode } from "react";

const useStyles = makeStyles((theme) =>
  createStyles({
    root: {
      backgroundColor: theme.palette.background.paper,
      border: "1px solid #dadde9",
      color: theme.palette.text.primary,
      padding: theme.spacing(1),
    },
  }),
);

export const StyledTooltip = (props: TooltipProps) => {
  const classes = useStyles();

  return <Tooltip classes={{ tooltip: classes.root }} {...props} />;
};

const useHelpInfoStyles = makeStyles((theme) =>
  createStyles({
    helpIcon: {
      color: theme.palette.grey[500],
    },
  }),
);

type HelpInfoProps = {
  children: NonNullable<ReactNode>;
  className?: string;
};

export const HelpInfo = ({ children, className }: HelpInfoProps) => {
  const classes = useHelpInfoStyles();

  return (
    <StyledTooltip className={className} title={children}>
      <HelpOutlineIcon fontSize="small" className={classes.helpIcon} />
    </StyledTooltip>
  );
};
