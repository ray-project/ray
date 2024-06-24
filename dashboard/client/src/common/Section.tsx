import {
  Box,
  BoxProps,
  Paper,
  Theme,
  Typography,
  useTheme,
} from "@mui/material";
import React, { PropsWithChildren } from "react";
import { ClassNameProps } from "./props";

const useStyles = (theme: Theme) => ({
  contentContainer: (noTopPadding: boolean) => ({
    padding: theme.spacing(2),
    height: "100%",
    paddingTop: noTopPadding ? 0 : undefined,
  }),
});

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
  const styles = useStyles(useTheme());

  return (
    <Box className={className} {...props}>
      {title && (
        <Box paddingBottom={2}>
          <Typography variant="h4">{title}</Typography>
        </Box>
      )}
      <Paper
        variant="outlined"
        sx={Object.assign({}, styles.contentContainer(noTopPadding))}
      >
        {children}
      </Paper>
    </Box>
  );
};
