import {
  createStyles,
  makeStyles,
  Tooltip,
  TooltipProps,
} from "@material-ui/core";
import HelpOutlineIcon from "@material-ui/icons/HelpOutline";
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
    <StyledTooltip className={className} interactive title={children}>
      <HelpOutlineIcon fontSize="small" className={classes.helpIcon} />
    </StyledTooltip>
  );
};
