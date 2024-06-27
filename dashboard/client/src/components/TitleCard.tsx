import { Box, Paper } from "@mui/material";
import React, { PropsWithChildren, ReactNode } from "react";

const TitleCard = ({
  title,
  children,
}: PropsWithChildren<{ title?: ReactNode | string }>) => {
  return (
    <Paper
      sx={{
        padding: 2,
        paddingTop: 1.5,
        margin: (theme) => theme.spacing(2, 1),
      }}
      elevation={0}
    >
      {title && (
        <Box
          sx={(theme) => ({
            fontSize: theme.typography.fontSize + 2,
            fontWeight: 500,
            color: theme.palette.text.secondary,
            marginBottom: 1,
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
