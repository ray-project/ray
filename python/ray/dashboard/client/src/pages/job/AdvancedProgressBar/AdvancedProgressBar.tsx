import {
  Box,
  Link,
  SxProps,
  Table,
  TableBody,
  TableCell,
  TableRow,
  Theme,
} from "@mui/material";
import React, { useState } from "react";
import {
  RiAddLine,
  RiArrowDownSLine,
  RiArrowRightSLine,
  RiCloseLine,
  RiSubtractLine,
} from "react-icons/ri";
import { Link as RouterLink } from "react-router-dom";
import { ClassNameProps } from "../../../common/props";
import { JobProgressGroup, NestedJobProgressLink } from "../../../type/job";
import { MiniTaskProgressBar } from "../TaskProgressBar";

export type AdvancedProgressBarProps = {
  progressGroups: JobProgressGroup[] | undefined;
  sx?: SxProps<Theme>;
} & ClassNameProps &
  Pick<AdvancedProgressBarSegmentProps, "onClickLink">;

export const AdvancedProgressBar = ({
  progressGroups,
  className,
  sx,
  ...segmentProps
}: AdvancedProgressBarProps) => {
  return (
    <Table className={className} sx={sx}>
      <TableBody>
        {progressGroups !== undefined ? (
          progressGroups.map((group) => (
            <AdvancedProgressBarSegment
              key={group.key}
              jobProgressGroup={group}
              {...segmentProps}
            />
          ))
        ) : (
          <TableRow>
            <TableCell>Loading...</TableCell>
          </TableRow>
        )}
      </TableBody>
    </Table>
  );
};

export type AdvancedProgressBarSegmentProps = {
  jobProgressGroup: JobProgressGroup;
  /**
   * Whether the segment should be expanded or not.
   * Only applies to this segment and not it's children.
   */
  startExpanded?: boolean;
  /**
   * How nested this segment is.
   * By default, we assume this is a top level segment.
   */
  nestedIndex?: number;
  /**
   * Whether to show a collapse button to the left. Used to collapse the parent.
   * This is a special case for "GROUP"s
   */
  showParentCollapseButton?: boolean;
  onParentCollapseButtonPressed?: () => void;
  onClickLink?: (link: NestedJobProgressLink) => void;
};

export const AdvancedProgressBarSegment = ({
  jobProgressGroup: { name, progress, children, type, link },
  startExpanded = false,
  nestedIndex = 1,
  showParentCollapseButton = false,
  onParentCollapseButtonPressed,
  onClickLink,
}: AdvancedProgressBarSegmentProps) => {
  const [expanded, setExpanded] = useState(startExpanded);
  const isGroup = type === "GROUP";

  const IconComponent = isGroup
    ? expanded
      ? RiSubtractLine
      : RiAddLine
    : expanded
    ? RiArrowDownSLine
    : RiArrowRightSLine;

  const showCollapse = isGroup && expanded;
  const handleCollapse = showCollapse
    ? () => {
        setExpanded(false);
      }
    : undefined;

  return (
    <React.Fragment>
      {/* Don't show the "GROUP" type rows if it's expanded. We only show the children */}
      {isGroup && expanded ? null : (
        <TableRow>
          <TableCell
            sx={{
              paddingLeft: 0,
              whiteSpace: "nowrap",
              display: "flex",
              alignItems: "center",
            }}
            onClick={() => {
              setExpanded(!expanded);
            }}
          >
            {showParentCollapseButton && (
              <Box
                component={RiSubtractLine}
                title="Collapse group"
                onClick={onParentCollapseButtonPressed}
                sx={{
                  width: 16,
                  height: 16,
                  verticalAlign: "top",
                  marginRight: 0.5,
                  marginLeft: 3 * (nestedIndex - 1),
                }}
              />
            )}
            <Box
              component={IconComponent}
              title={expanded ? "Collapse" : "Expand"}
              sx={{
                width: 16,
                height: 16,
                verticalAlign: "top",
                marginRight: 0.5,
                visibility: children.length === 0 ? "hidden" : "visible",
              }}
              style={{
                // Complex logic on where to place the icon depending on the grouping type
                marginLeft: showParentCollapseButton
                  ? 4
                  : 24 * (isGroup ? nestedIndex - 1 : nestedIndex),
                marginRight: isGroup ? 28 : 4,
              }}
            />
            {link ? (
              link.type === "actor" ? (
                <Box
                  component="button"
                  sx={{
                    border: "none",
                    cursor: "pointer",
                    color: "#036DCF",
                    textDecoration: "underline",
                    background: "none",
                  }}
                  onClick={(event) => {
                    onClickLink?.(link);
                    event.stopPropagation();
                  }}
                >
                  {name}
                </Box>
              ) : (
                <Link
                  component={RouterLink}
                  sx={{
                    border: "none",
                    cursor: "pointer",
                    color: "#036DCF",
                    textDecoration: "underline",
                    background: "none",
                  }}
                  to={`tasks/${link.id}`}
                >
                  {name}
                </Link>
              )
            ) : (
              name
            )}
            {isGroup && (
              <React.Fragment>
                <Box component="span" sx={{ width: 4 }} />
                {"("}
                <RiCloseLine /> {children.length}
                {")"}
              </React.Fragment>
            )}
          </TableCell>
          <TableCell sx={{ width: "100%", paddingRight: 0 }}>
            <MiniTaskProgressBar {...progress} showTotal />
          </TableCell>
        </TableRow>
      )}
      {expanded &&
        children.map((child, index) => (
          <AdvancedProgressBarSegment
            key={child.key}
            jobProgressGroup={child}
            nestedIndex={isGroup ? nestedIndex : nestedIndex + 1}
            showParentCollapseButton={showCollapse && index === 0}
            onParentCollapseButtonPressed={handleCollapse}
            onClickLink={onClickLink}
          />
        ))}
    </React.Fragment>
  );
};
