import { createStyles, makeStyles, Paper } from "@material-ui/core";
import classNames from "classnames";
import React, { PropsWithChildren } from "react";

const useStyles = makeStyles((theme) =>
  createStyles({
    root: {
      padding: theme.spacing(2, 3),
      width: 448,
      height: 270,
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
