import { Paper, styled } from "@mui/material";
import React, { PropsWithChildren, ReactNode } from "react";

const CardPaper = styled(Paper)(({ theme }) => ({
  padding: theme.spacing(2),
  paddingTop: theme.spacing(1.5),
  margin: theme.spacing(2, 1),
}));

const TitleDiv = styled("div")(({ theme }) => ({
  fontSize: theme.typography.fontSize + 2,
  fontWeight: 500,
  color: theme.palette.text.secondary,
  marginBottom: theme.spacing(1),
}));

const BodyDiv = styled("div")("");

const TitleCard = ({
  title,
  children,
}: PropsWithChildren<{ title?: ReactNode | string }>) => {
  return (
    <CardPaper elevation={0}>
      {title && <TitleDiv>{title}</TitleDiv>}
      <BodyDiv>{children}</BodyDiv>
    </CardPaper>
  );
};

export default TitleCard;
