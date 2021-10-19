import { makeStyles, Paper } from "@material-ui/core";
import React, { PropsWithChildren, ReactNode } from "react";

const useStyles = makeStyles((theme) => ({
  card: {
    padding: theme.spacing(2),
    paddingTop: theme.spacing(1.5),
    margin: [theme.spacing(2), theme.spacing(1)].map((e) => `${e}px`).join(" "),
  },
  title: {
    fontSize: theme.typography.fontSize + 2,
    fontWeight: 500,
    color: theme.palette.text.secondary,
    marginBottom: theme.spacing(1),
  },
  body: {
    padding: theme.spacing(0.5),
  },
}));

const TitleCard = ({
  title,
  children,
}: PropsWithChildren<{ title: ReactNode | string }>) => {
  const classes = useStyles();
  return (
    <Paper className={classes.card}>
      <div className={classes.title}>{title}</div>
      <div className={classes.body}>{children}</div>
    </Paper>
  );
};

export default TitleCard;
