import { Box, Paper, Theme, useTheme } from "@mui/material";
import React, { PropsWithChildren, ReactNode } from "react";

const useStyles = (theme: Theme) => ({
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
});

const TitleCard = ({
  title,
  children,
}: PropsWithChildren<{ title?: ReactNode | string }>) => {
  const styles = useStyles(useTheme());
  return (
    <Paper sx={styles.card} elevation={0}>
      {title && <Box sx={styles.title}>{title}</Box>}
      <Box sx={styles.body}>{children}</Box>
    </Paper>
  );
};

export default TitleCard;
