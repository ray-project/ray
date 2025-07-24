import { Box, Link, Paper, SxProps, Theme, Typography } from "@mui/material";
import React, { PropsWithChildren } from "react";
import { RiArrowRightLine } from "react-icons/ri";
import { Link as RouterLink } from "react-router-dom";

type OverviewCardProps = PropsWithChildren<{
  className?: string;
  sx?: SxProps<Theme>;
}>;

export const OverviewCard = ({
  children,
  className,
  sx,
}: OverviewCardProps) => {
  return (
    <Paper
      className={className}
      sx={[
        { height: 400, overflow: "hidden" },
        ...(Array.isArray(sx) ? sx : [sx]),
      ]}
      variant="outlined"
    >
      {children}
    </Paper>
  );
};

type LinkWithArrowProps = {
  text: string;
  to: string;
};

export const LinkWithArrow = ({ text, to }: LinkWithArrowProps) => {
  return (
    <Link
      component={RouterLink}
      sx={{
        color: "#036DCF",
        textDecoration: "none",
        display: "flex",
        flexDirection: "row",
        flexWrap: "nowrap",
        alignItems: "center",
      }}
      to={to}
    >
      <Typography variant="h4">{text}</Typography>
      <Box
        component={RiArrowRightLine}
        sx={{
          marginLeft: 0.5,
          width: 24,
          height: 24,
        }}
      />
    </Link>
  );
};
