import { Box, SxProps, Theme, Typography } from "@mui/material";
import React, {
  forwardRef,
  PropsWithChildren,
  useEffect,
  useState,
} from "react";
import { RiArrowDownSLine, RiArrowRightSLine } from "react-icons/ri";
import { ClassNameProps } from "./props";

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
    sx?: SxProps<Theme>;
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
      sx,
    },
    ref,
  ) => {
    const [internalExpanded, setInternalExpanded] = useState(startExpanded);
    const finalExpanded = expanded !== undefined ? expanded : internalExpanded;

    const handleExpandClick = () => {
      onExpandButtonClick?.();
      setInternalExpanded(!finalExpanded);
    };

    return (
      <Box ref={ref} className={className} sx={sx}>
        <Box display="flex" flexDirection="row" alignItems="center">
          <Typography
            sx={{
              display: "flex",
              flexDirection: "row",
              flexWrap: "nowrap",
              alignItems: "center",
              fontWeight: 500,
              cursor: "pointer",
              marginRight: 1,
            }}
            variant="h4"
            onClick={handleExpandClick}
          >
            {finalExpanded ? (
              <Box
                component={RiArrowDownSLine}
                sx={{
                  marginRight: 1,
                  width: 24,
                  height: 24,
                }}
              />
            ) : (
              <Box
                component={RiArrowRightSLine}
                sx={{
                  marginRight: 1,
                  width: 24,
                  height: 24,
                }}
              />
            )}
            {title}
          </Typography>
          {icon}
        </Box>
        <HideableBlock visible={finalExpanded} keepRendered={keepRendered}>
          {children}
        </HideableBlock>
      </Box>
    );
  },
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
    <Box
      sx={{
        marginTop: 1,
        display: !visible ? "none" : "block",
      }}
    >
      {children}
    </Box>
  ) : null;
};
