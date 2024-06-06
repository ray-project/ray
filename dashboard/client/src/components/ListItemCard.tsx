import { Typography } from "@mui/material";
import { styled } from "@mui/material/styles";
import React, { ReactNode } from "react";
import { Link } from "react-router-dom";
import { ClassNameProps } from "../common/props";
import {
  LinkWithArrow,
  OverviewCard,
} from "../pages/overview/cards/OverviewCard";

type ListItemCardProps = {
  headerTitle: string;
  items: ListItemProps[];
  emptyListText: string;
  footerText: string;
  footerLink: string;
} & ClassNameProps;

type ListItemProps = {
  title: string | undefined;
  subtitle: string;
  link: string | undefined;
  icon: ReactNode;
} & ClassNameProps;

const ListItemCardRoot = styled(OverviewCard)(({ theme }) => ({
  display: "flex",
  flexDirection: "column",
  padding: theme.spacing(2, 3),
}));

const ListContainerDiv = styled("div")(({ theme }) => ({
  marginTop: theme.spacing(2),
  flex: 1,
  overflow: "hidden",
}));

export const ListItemCard = ({
  className,
  headerTitle,
  items,
  emptyListText: itemEmptyTip,
  footerText,
  footerLink,
}: ListItemCardProps) => {
  return (
    <ListItemCardRoot className={className}>
      <Typography variant="h3">{headerTitle}</Typography>
      <ListContainerDiv>
        {items.map((item: ListItemProps) => (
          <StyledListItem {...item} key={item.title} />
        ))}
        {items.length === 0 && (
          <Typography variant="h4">{itemEmptyTip}</Typography>
        )}
      </ListContainerDiv>
      <LinkWithArrow text={footerText} to={footerLink} />
    </ListItemCardRoot>
  );
};

const ListItemRootLink = styled(Link)(({ theme }) => ({
  display: "flex",
  flexDirection: "row",
  flexWrap: "nowrap",
  alignItems: "center",
  textDecoration: "none",
}));

const ListItemRootDiv = styled("div")(({ theme }) => ({
  display: "flex",
  flexDirection: "row",
  flexWrap: "nowrap",
  alignItems: "center",
  textDecoration: "none",
}));

const TextContainerDiv = styled("div")(({ theme }) => ({
  flex: "1 1 auto",
  width: `calc(100% - calc(${theme.spacing(1)} + 20px))`,
}));

const TitleTypography = styled(Typography)(({ theme }) => ({
  color: "#036DCF",
}));

const EntrypointTypography = styled(Typography)(({ theme }) => ({
  overflow: "hidden",
  textOverflow: "ellipsis",
  whiteSpace: "nowrap",
  color: "#5F6469",
}));

const ListItem = ({
  icon,
  title,
  subtitle,
  className,
  link,
}: ListItemProps) => {
  const cardContent = (
    <React.Fragment>
      {icon}
      <TextContainerDiv>
        <TitleTypography variant="body2">{title}</TitleTypography>
        <EntrypointTypography title={subtitle} variant="caption">
          {subtitle}
        </EntrypointTypography>
      </TextContainerDiv>
    </React.Fragment>
  );
  return (
    <div className={className}>
      {link !== undefined ? (
        <ListItemRootLink to={link}>{cardContent}</ListItemRootLink>
      ) : (
        <ListItemRootDiv>{cardContent}</ListItemRootDiv>
      )}
    </div>
  );
};

const StyledListItem = styled(ListItem)(({ theme }) => ({
  "&:not(:first-of-type)": {
    marginTop: theme.spacing(1),
  },
}));
