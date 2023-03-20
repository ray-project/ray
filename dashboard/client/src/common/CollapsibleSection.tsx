import { createStyles, makeStyles, Typography } from "@material-ui/core";
import classNames from "classnames";
import React, {
  forwardRef,
  PropsWithChildren,
  useEffect,
  useState,
} from "react";
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
    /**
     * Allows the parent component to control if this section is expanded.
     * If undefined, the child wil own the expansion state
     */
    expanded?: boolean;
    onExpandButtonClick?: () => void;
    title: string;
    startExpanded?: boolean;
    /**
     * An optimization to not avoid re-rendering the contents of the collapsible section.
     * When enabled, we will keep the content around when collapsing but hide it via css.
     */
    keepRendered?: boolean;
  } & ClassNameProps
>;

export const CollapsibleSection = forwardRef<
  HTMLDivElement,
  CollapsibleSectionProps
>(
  (
    {
      title,
      expanded,
      onExpandButtonClick,
      startExpanded = false,
      className,
      children,
      keepRendered,
    },
    ref,
  ) => {
    const classes = useStyles();
    const [internalExpanded, setInternalExpanded] = useState(startExpanded);
    const finalExpanded = expanded !== undefined ? expanded : internalExpanded;
    const [rendered, setRendered] = useState(finalExpanded);

    useEffect(() => {
      if (finalExpanded) {
        setRendered(true);
      }
    }, [finalExpanded]);

    const handleExpandClick = () => {
      onExpandButtonClick?.();
      setInternalExpanded(!finalExpanded);
    };

    return (
      <div ref={ref} className={className}>
        <Typography
          className={classes.title}
          variant="h4"
          onClick={handleExpandClick}
        >
          {finalExpanded ? (
            <RiArrowDownSLine className={classes.icon} />
          ) : (
            <RiArrowRightSLine className={classes.icon} />
          )}
          {title}
        </Typography>
        {(finalExpanded || (keepRendered && rendered)) && (
          <div
            className={classNames(classes.body, {
              [classes.bodyHidden]: !finalExpanded,
            })}
          >
            {children}
          </div>
        )}
      </div>
    );
  },
);
