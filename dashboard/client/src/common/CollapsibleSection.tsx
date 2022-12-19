import { createStyles, makeStyles, Typography } from "@material-ui/core";
import React, { PropsWithChildren, useState } from "react";
import { RiArrowDownSLine, RiArrowUpSLine } from "react-icons/ri";

const useStyles = makeStyles((theme) =>
  createStyles({
    title: {
      display: "flex",
      flexDirection: "row",
      flexWrap: "nowrap",
      alignItems: "center",
      fontWeight: 500,
    },
    icon: {
      marginRight: theme.spacing(1),
      width: 24,
      height: 24,
    },
    body: {
      marginTop: theme.spacing(3),
    },
  }),
);

type CollapsibleSectionProps = PropsWithChildren<{
  title: string;
  startExpanded?: boolean;
  className?: string;
}>;

export const CollapsibleSection = ({
  title,
  startExpanded = false,
  className,
  children,
}: CollapsibleSectionProps) => {
  const classes = useStyles();
  const [expanded, setExpanded] = useState(startExpanded);

  const handleExpandClick = () => {
    setExpanded(!expanded);
  };

  return (
    <div className={className}>
      <Typography
        className={classes.title}
        variant="h4"
        onClick={handleExpandClick}
      >
        {expanded ? (
          <RiArrowDownSLine className={classes.icon} />
        ) : (
          <RiArrowUpSLine className={classes.icon} />
        )}
        {title}
      </Typography>
      {expanded && <div className={classes.body}>{children}</div>}
    </div>
  );
};
