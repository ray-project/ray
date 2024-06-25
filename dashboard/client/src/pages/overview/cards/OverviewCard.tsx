import {
  Box,
  Link,
  Paper,
  SxProps,
  Theme,
  Typography,
  useTheme,
} from "@mui/material";
import React, { PropsWithChildren } from "react";
import { RiArrowRightLine } from "react-icons/ri";
import { Link as RouterLink } from "react-router-dom";

const useStyles = (theme: Theme) => ({
  root: {
    height: 400,
    overflow: "hidden",
  },
});

type OverviewCardProps = PropsWithChildren<{
  className?: string;
  sx?: SxProps<Theme>;
}>;

export const OverviewCard = ({
  children,
  className,
  sx,
}: OverviewCardProps) => {
  const styles = useStyles(useTheme());
  return (
    <Paper
      className={className}
      sx={[styles.root, ...(Array.isArray(sx) ? sx : [sx])]}
      variant="outlined"
    >
      {children}
    </Paper>
  );
};

const useLinkWithArrowStyles = (theme: Theme) => ({
  root: {
    color: "#036DCF",
    textDecoration: "none",
    display: "flex",
    flexDirection: "row",
    flexWrap: "nowrap",
    alignItems: "center",
  },
  icon: {
    marginLeft: theme.spacing(0.5),
    width: 24,
    height: 24,
  },
});

type LinkWithArrowProps = {
  text: string;
  to: string;
};

export const LinkWithArrow = ({ text, to }: LinkWithArrowProps) => {
  const styles = useLinkWithArrowStyles(useTheme());
  return (
    <Link component={RouterLink} sx={styles.root} to={to}>
      <Typography variant="h4">{text}</Typography>
      <Box component={RiArrowRightLine} sx={styles.icon} />
    </Link>
  );
};
