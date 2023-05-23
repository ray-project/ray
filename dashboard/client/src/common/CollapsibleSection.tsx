import { Box, createStyles, makeStyles, Typography } from "@material-ui/core";
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
      marginRight: theme.spacing(1),
    },
    icon: {
      marginRight: theme.spacing(1),
      width: 24,
      height: 24,
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
     * Icon to show to the right of the title.
     */
    icon?: React.ReactNode;
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
      icon,
    },
    ref,
  ) => {
    const classes = useStyles();
    const [internalExpanded, setInternalExpanded] = useState(startExpanded);
    const finalExpanded = expanded !== undefined ? expanded : internalExpanded;

    const handleExpandClick = () => {
      onExpandButtonClick?.();
      setInternalExpanded(!finalExpanded);
    };

    return (
      <div ref={ref} className={className}>
        <Box display="flex" flexDirection="row" alignItems="center">
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
          {icon}
        </Box>
        <HideableBlock visible={finalExpanded} keepRendered={keepRendered}>
          {children}
        </HideableBlock>
      </div>
    );
  },
);

const useHideableBlockStyles = makeStyles((theme) =>
  createStyles({
    body: {
      marginTop: theme.spacing(1),
    },
    bodyHidden: {
      display: "none",
    },
  }),
);

type HideableBlockProps = PropsWithChildren<
  {
    visible: boolean;
    /**
     * An optimization to not avoid re-rendering the contents of the collapsible section.
     * When enabled, we will keep the content around when collapsing but hide it via css.
     */
    keepRendered?: boolean;
  } & ClassNameProps
>;

/**
 * Component that can be hidden depending on a passed in prop. Supports an optimization
 * to keep the component rendered (but not visible) when hidden to avoid re-rendering
 * when component is shown again.
 */
export const HideableBlock = ({
  visible,
  keepRendered,
  children,
}: HideableBlockProps) => {
  const classes = useHideableBlockStyles();

  // visible represents whether the component is viewable in the browser.
  // Rendered represents whether the DOM elements exist in the DOM tree.
  // If !visible && rendered, then the elements are in the DOM but are
  // not drawn via CSS visibility rules.
  const [rendered, setRendered] = useState(visible);

  useEffect(() => {
    if (visible) {
      setRendered(true);
    }
  }, [visible]);

  // Optimization to keep the component rendered (but not visible) when hidden
  // to avoid re-rendering when component is shown again.
  return visible || (keepRendered && rendered) ? (
    <div
      className={classNames(classes.body, {
        [classes.bodyHidden]: !visible,
      })}
    >
      {children}
    </div>
  ) : null;
};
