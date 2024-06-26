import { Box, Paper } from "@mui/material";
import React, { PropsWithChildren, ReactNode } from "react";

const TitleCard = ({
  title,
  children,
}: PropsWithChildren<{ title?: ReactNode | string }>) => {
  return (
    <Paper
      sx={(theme) => ({
        padding: theme.spacing(2),
        paddingTop: theme.spacing(1.5),
        margin: theme.spacing(2, 1),
      })}
      elevation={0}
    >
      {title && (
        <Box
          sx={(theme) => ({
            fontSize: theme.typography.fontSize + 2,
            fontWeight: 500,
            color: theme.palette.text.secondary,
            marginBottom: theme.spacing(1),
          })}
        >
          {title}
        </Box>
      )}
      <Box>{children}</Box>
    </Paper>
  );
};

export default TitleCard;
