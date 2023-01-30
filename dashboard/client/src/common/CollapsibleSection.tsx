import { createStyles, makeStyles, Typography } from "@material-ui/core";
import classNames from "classnames";
import React, { PropsWithChildren, useEffect, useState } from "react";
import { RiArrowDownSLine, RiArrowRightSLine } from "react-icons/ri";
import { ClassNameProps } from "./props";

const useStyles = makeStyles((theme) =>
  createStyles({
    title: {
      display: "flex",
      flexDirection: "row",
      flexWrap: "nowrap",
      alignItems: "center",
      fontWeight: 500,
      cursor: "pointer",
    },
    icon: {
      marginRight: theme.spacing(1),
      width: 24,
      height: 24,
    },
    body: {
      marginTop: theme.spacing(1),
    },
    bodyHidden: {
      display: "none",
    },
  }),
);

type CollapsibleSectionProps = PropsWithChildren<
  {
    title: string;
    startExpanded?: boolean;
    /**
     * An optimization to not avoid re-rendering the contents of the collapsible section.
     * When enabled, we will keep the content around when collapsing but hide it via css.
     */
    keepRendered?: boolean;
  } & ClassNameProps
>;

export const CollapsibleSection = ({
  title,
  startExpanded = false,
  className,
  children,
  keepRendered,
}: CollapsibleSectionProps) => {
  const classes = useStyles();
  const [expanded, setExpanded] = useState(startExpanded);
  const [rendered, setRendered] = useState(expanded);

  useEffect(() => {
    if (expanded) {
      setRendered(true);
    }
  }, [expanded]);

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
          <RiArrowRightSLine className={classes.icon} />
        )}
        {title}
      </Typography>
      {(expanded || (keepRendered && rendered)) && (
        <div
          className={classNames(classes.body, {
            [classes.bodyHidden]: !expanded,
          })}
        >
          {children}
        </div>
      )}
    </div>
  );
};
