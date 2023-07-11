import {
  Box,
  BoxProps,
  createStyles,
  makeStyles,
  Paper,
  Typography,
} from "@material-ui/core";
import classNames from "classnames";
import React, { PropsWithChildren } from "react";
import { ClassNameProps } from "./props";

const useStyles = makeStyles((theme) =>
  createStyles({
    contentContainer: {
      padding: theme.spacing(2),
      height: "100%",
    },
    contentContainerNoTopPadding: {
      paddingTop: 0,
    },
  }),
);

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
  const classes = useStyles();

  return (
    <Box className={className} {...props}>
      {title && (
        <Box paddingBottom={2}>
          <Typography variant="h4">{title}</Typography>
        </Box>
      )}
      <Paper
        variant="outlined"
        className={classNames(classes.contentContainer, {
          [classes.contentContainerNoTopPadding]: noTopPadding,
        })}
      >
        {children}
      </Paper>
    </Box>
  );
};
