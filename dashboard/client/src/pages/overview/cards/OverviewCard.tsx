import { Paper, Typography } from "@mui/material";
import { styled } from "@mui/material/styles";
import React, { PropsWithChildren } from "react";
import { RiArrowRightLine } from "react-icons/ri";
import { Link } from "react-router-dom";

const OverviewCardRoot = styled(Paper)(({ theme }) => ({
  height: 400,
  overflow: "hidden",
}));

type OverviewCardProps = PropsWithChildren<{
  className?: string;
}>;

export const OverviewCard = ({ children, className }: OverviewCardProps) => {
  return (
    <OverviewCardRoot className={className} variant="outlined">
      {children}
    </OverviewCardRoot>
  );
};

const LinkWithArrowRoot = styled(Link)(({ theme }) => ({
  color: "#036DCF",
  textDecoration: "none",
  display: "flex",
  flexDirection: "row",
  flexWrap: "nowrap",
  alignItems: "center",
}));

const LinkWithArrowIcon = styled(RiArrowRightLine)(({ theme }) => ({
  marginLeft: theme.spacing(0.5),
  width: 24,
  height: 24,
}));

type LinkWithArrowProps = {
  text: string;
  to: string;
};

export const LinkWithArrow = ({ text, to }: LinkWithArrowProps) => {
  return (
    <LinkWithArrowRoot to={to}>
      <Typography variant="h4">{text}</Typography>
      <LinkWithArrowIcon />
    </LinkWithArrowRoot>
  );
};
