import { Box, Link, SxProps, Theme, Typography } from "@mui/material";
import React, { ReactNode } from "react";
import { Link as RouterLink } from "react-router-dom";
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
  sx?: SxProps<Theme>;
} & ClassNameProps;

type ListItemProps = {
  title: string | undefined;
  subtitle: string;
  link: string | undefined;
  icon: ReactNode;
  sx?: SxProps<Theme>;
} & ClassNameProps;

export const ListItemCard = ({
  className,
  headerTitle,
  items,
  emptyListText: itemEmptyTip,
  footerText,
  footerLink,
  sx,
}: ListItemCardProps) => {
  return (
    <OverviewCard
      className={className}
      sx={[
        {
          display: "flex",
          flexDirection: "column",
          paddingX: 3,
          paddingY: 2,
        },
        ...(Array.isArray(sx) ? sx : [sx]),
      ]}
    >
      <Typography variant="h3">{headerTitle}</Typography>
      <Box
        sx={{
          marginTop: 2,
          flex: 1,
          overflow: "hidden",
        }}
      >
        {items.map((item: ListItemProps) => (
          <ListItem
            {...item}
            sx={{
              "&:not(:first-child)": {
                marginTop: 1,
              },
            }}
            key={item.title}
          />
        ))}
        {items.length === 0 && (
          <Typography variant="h4">{itemEmptyTip}</Typography>
        )}
      </Box>
      <LinkWithArrow text={footerText} to={footerLink} />
    </OverviewCard>
  );
};

const listItemStyles = {
  root: {
    display: "flex",
    flexDirection: "row",
    flexWrap: "nowrap",
    alignItems: "center",
    textDecoration: "none",
  },
};

const ListItem = ({
  icon,
  title,
  subtitle,
  className,
  link,
  sx,
}: ListItemProps) => {
  const cardContent = (
    <React.Fragment>
      {icon}
      <Box
        sx={(theme) => ({
          flex: "1 1 auto",
          width: `calc(100% - calc(${theme.spacing(1)} + 20px))`,
        })}
      >
        <Typography sx={{ color: "#036DCF" }} variant="body2">
          {title}
        </Typography>
        <Typography
          sx={{
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
            color: "#5F6469",
          }}
          title={subtitle}
          variant="caption"
        >
          {subtitle}
        </Typography>
      </Box>
    </React.Fragment>
  );
  return (
    <Box className={className} sx={sx}>
      {link !== undefined ? (
        <Link component={RouterLink} sx={listItemStyles.root} to={link}>
          {cardContent}
        </Link>
      ) : (
        <Box sx={listItemStyles.root}>{cardContent}</Box>
      )}
    </Box>
  );
};
