import { Box, Link, SxProps, Theme, Typography, useTheme } from "@mui/material";
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

const useStyles = (theme: Theme) => ({
  root: {
    display: "flex",
    flexDirection: "column",
    padding: theme.spacing(2, 3),
  },
  listContainer: {
    marginTop: theme.spacing(2),
    flex: 1,
    overflow: "hidden",
  },
  listItem: {
    "&:not(:first-child)": {
      marginTop: theme.spacing(1),
    },
  },
});

export const ListItemCard = ({
  className,
  headerTitle,
  items,
  emptyListText: itemEmptyTip,
  footerText,
  footerLink,
  sx,
}: ListItemCardProps) => {
  const styles = useStyles(useTheme());

  return (
    <OverviewCard className={className} sx={Object.assign({}, styles.root, sx)}>
      <Typography variant="h3">{headerTitle}</Typography>
      <Box sx={styles.listContainer}>
        {items.map((item: ListItemProps) => (
          <ListItem {...item} sx={styles.listItem} key={item.title} />
        ))}
        {items.length === 0 && (
          <Typography variant="h4">{itemEmptyTip}</Typography>
        )}
      </Box>
      <LinkWithArrow text={footerText} to={footerLink} />
    </OverviewCard>
  );
};

const useListItemStyles = (theme: Theme) => ({
  root: {
    display: "flex",
    flexDirection: "row",
    flexWrap: "nowrap",
    alignItems: "center",
    textDecoration: "none",
  },

  textContainer: {
    flex: "1 1 auto",
    width: `calc(100% - calc(${theme.spacing(1)} + 20px))`,
  },
  title: {
    color: "#036DCF",
  },
  entrypoint: {
    overflow: "hidden",
    textOverflow: "ellipsis",
    whiteSpace: "nowrap",
    color: "#5F6469",
  },
});

const ListItem = ({
  icon,
  title,
  subtitle,
  className,
  link,
  sx,
}: ListItemProps) => {
  const styles = useListItemStyles(useTheme());

  const cardContent = (
    <React.Fragment>
      {icon}
      <Box sx={styles.textContainer}>
        <Typography sx={styles.title} variant="body2">
          {title}
        </Typography>
        <Typography sx={styles.entrypoint} title={subtitle} variant="caption">
          {subtitle}
        </Typography>
      </Box>
    </React.Fragment>
  );
  return (
    <Box className={className} sx={sx}>
      {link !== undefined ? (
        <Link component={RouterLink} sx={styles.root} to={link}>
          {cardContent}
        </Link>
      ) : (
        <Box sx={styles.root}>{cardContent}</Box>
      )}
    </Box>
  );
};
