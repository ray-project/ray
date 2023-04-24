import {
  Box,
  BoxProps,
  createStyles,
  makeStyles,
  Paper,
  Typography,
} from "@material-ui/core";
import React, { PropsWithChildren } from "react";
import { ClassNameProps } from "./props";

const useStyles = makeStyles((theme) =>
  createStyles({
    contentContainer: {
      padding: theme.spacing(2),
      height: "100%",
    },
  }),
);

type SectionProps = {
  title?: string;
} & ClassNameProps &
  BoxProps;

export const Section = ({
  title,
  children,
  className,
  ...props
}: PropsWithChildren<SectionProps>) => {
  const classes = useStyles();

  return (
    <Box className={className} {...props}>
      {title && (
        <Box paddingBottom={2}>
          <Typography variant="h4">{title}</Typography>
        </Box>
      )}
      <Paper variant="outlined" className={classes.contentContainer}>
        {children}
      </Paper>
    </Box>
  );
};
