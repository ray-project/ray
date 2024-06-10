import { Paper } from "@mui/material";
import makeStyles from "@mui/styles/makeStyles";
import React, { PropsWithChildren, ReactNode } from "react";

const useStyles = makeStyles((theme) => ({
  card: {
    padding: theme.spacing(2),
    paddingTop: theme.spacing(1.5),
    margin: theme.spacing(2, 1),
  },
  title: {
    fontSize: theme.typography.fontSize + 2,
    fontWeight: 500,
    color: theme.palette.text.secondary,
    marginBottom: theme.spacing(1),
  },
  body: {},
}));

const TitleCard = ({
  title,
  children,
}: PropsWithChildren<{ title?: ReactNode | string }>) => {
  const classes = useStyles();
  return (
    <Paper className={classes.card} elevation={0}>
      {title && <div className={classes.title}>{title}</div>}
      <div className={classes.body}>{children}</div>
    </Paper>
  );
};

export default TitleCard;
