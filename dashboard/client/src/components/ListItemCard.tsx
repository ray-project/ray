import { createStyles, makeStyles, Typography } from "@material-ui/core";
import classNames from "classnames";
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

const useStyles = makeStyles((theme) =>
  createStyles({
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
  }),
);

export const ListItemCard = ({
  className,
  headerTitle,
  items,
  emptyListText: itemEmptyTip,
  footerText,
  footerLink,
}: ListItemCardProps) => {
  const classes = useStyles();

  return (
    <OverviewCard className={classNames(classes.root, className)}>
      <Typography variant="h3">{headerTitle}</Typography>
      <div className={classes.listContainer}>
        {items.map((item: ListItemProps) => (
          <ListItem {...item} className={classes.listItem} key={item.title} />
        ))}
        {items.length === 0 && (
          <Typography variant="h4">{itemEmptyTip}</Typography>
        )}
      </div>
      <LinkWithArrow text={footerText} to={footerLink} />
    </OverviewCard>
  );
};

const useListItemStyles = makeStyles((theme) =>
  createStyles({
    root: {
      display: "flex",
      flexDirection: "row",
      flexWrap: "nowrap",
      alignItems: "center",
      textDecoration: "none",
    },

    textContainer: {
      flex: "1 1 auto",
      width: `calc(100% - ${theme.spacing(1) + 20}px)`,
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
  }),
);

const ListItem = ({
  icon,
  title,
  subtitle,
  className,
  link,
}: ListItemProps) => {
  const classes = useListItemStyles();

  const cardContent = (
    <React.Fragment>
      {icon}
      <div className={classes.textContainer}>
        <Typography className={classes.title} variant="body2">
          {title}
        </Typography>
        <Typography
          className={classes.entrypoint}
          title={subtitle}
          variant="caption"
        >
          {subtitle}
        </Typography>
      </div>
    </React.Fragment>
  );
  return (
    <div className={className}>
      {link !== undefined ? (
        <Link className={classes.root} to={link}>
          {cardContent}
        </Link>
      ) : (
        <div className={classes.root}>{cardContent}</div>
      )}
    </div>
  );
};
