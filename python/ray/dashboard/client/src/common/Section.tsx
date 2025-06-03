import { Box, BoxProps, Paper, Typography } from "@mui/material";
import React, { PropsWithChildren } from "react";
import { ClassNameProps } from "./props";

type SectionProps = {
  title?: string;
  noTopPadding?: boolean;
} & ClassNameProps &
  BoxProps;

export const Section = ({
  title,
  children,
  className,
  noTopPadding = false,
  ...props
}: PropsWithChildren<SectionProps>) => {
  return (
    <Box className={className} {...props}>
      {title && (
        <Box paddingBottom={2}>
          <Typography variant="h4">{title}</Typography>
        </Box>
      )}
      <Paper
        variant="outlined"
        sx={{
          padding: 2,
          height: "100%",
          paddingTop: noTopPadding ? 0 : undefined,
        }}
      >
        {children}
      </Paper>
    </Box>
  );
};
