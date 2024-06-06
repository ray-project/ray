import { Box, Typography } from "@mui/material";
import { styled } from "@mui/material/styles";
import React, {
  forwardRef,
  PropsWithChildren,
  useEffect,
  useState,
} from "react";
import { RiArrowDownSLine, RiArrowRightSLine } from "react-icons/ri";
import { ClassNameProps } from "./props";

const TitleTypography = styled(Typography)(({theme}) => ({
  display: "flex",
  flexDirection: "row",
  flexWrap: "nowrap",
  alignItems: "center",
  fontWeight: 500,
  cursor: "pointer",
  marginRight: theme.spacing(1),
}));

const SRiArrowDownSLine = styled(RiArrowDownSLine)(({theme}) => ({
  marginRight: theme.spacing(1),
  width: 24,
  height: 24,
}));

const SRiArrowRightSLine = styled(RiArrowRightSLine)(({theme}) => ({
  marginRight: theme.spacing(1),
  width: 24,
  height: 24,
}));

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
    const [internalExpanded, setInternalExpanded] = useState(startExpanded);
    const finalExpanded = expanded !== undefined ? expanded : internalExpanded;

    const handleExpandClick = () => {
      onExpandButtonClick?.();
      setInternalExpanded(!finalExpanded);
    };

    return (
      <div ref={ref} className={className}>
        <Box display="flex" flexDirection="row" alignItems="center">
          <TitleTypography
            variant="h4"
            onClick={handleExpandClick}
          >
            {finalExpanded ? (
              <SRiArrowDownSLine />
            ) : (
              <SRiArrowRightSLine />
            )}
            {title}
          </TitleTypography>
          {icon}
        </Box>
        <HideableBlock visible={finalExpanded} keepRendered={keepRendered}>
          {children}
        </HideableBlock>
      </div>
    );
  },
);

const HideableBlockDiv = styled("div")<{visible?:boolean}>(({theme, visible}) => ({
  marginTop: theme.spacing(1),
  display: visible ? "block" : "none",
}));

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
    <HideableBlockDiv visible={visible}>
      {children}
    </HideableBlockDiv>
  ) : null;
};
