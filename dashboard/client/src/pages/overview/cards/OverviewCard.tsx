import { createStyles, makeStyles, Paper, Typography } from "@material-ui/core";
import classNames from "classnames";
import React, { PropsWithChildren } from "react";
import { RiArrowRightLine } from "react-icons/ri";
import { Link } from "react-router-dom";

const useStyles = makeStyles((theme) =>
  createStyles({
    root: {
      padding: theme.spacing(2, 3),
      height: 294,
      borderRadius: 8,
    },
  }),
);

type OverviewCardProps = PropsWithChildren<{
  className?: string;
}>;

export const OverviewCard = ({ children, className }: OverviewCardProps) => {
  const classes = useStyles();
  return (
    <Paper
      className={classNames(classes.root, className)}
      elevation={1}
      variant="outlined"
    >
      {children}
    </Paper>
  );
};

const useLinkWithArrowStyles = makeStyles((theme) =>
  createStyles({
    root: {
      color: "#036DCF",
      textDecoration: "none",
      display: "flex",
      flexDirection: "row",
      flexWrap: "nowrap",
      alignItems: "center",
    },
    icon: {
      marginLeft: theme.spacing(0.5),
      width: 24,
      height: 24,
    },
  }),
);

type LinkWithArrowProps = {
  text: string;
  to: string;
};

export const LinkWithArrow = ({ text, to }: LinkWithArrowProps) => {
  const classes = useLinkWithArrowStyles();
  return (
    <Link className={classes.root} to={to}>
      <Typography variant="h4">{text}</Typography>
      <RiArrowRightLine className={classes.icon} />
    </Link>
  );
};
