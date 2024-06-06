import { Table, TableBody, TableCell, TableRow } from "@mui/material";
import { styled } from "@mui/material/styles";
import React, { useState } from "react";
import {
  RiAddLine,
  RiArrowDownSLine,
  RiArrowRightSLine,
  RiCloseLine,
  RiSubtractLine,
} from "react-icons/ri";
import { Link } from "react-router-dom";
import { ClassNameProps } from "../../../common/props";
import { JobProgressGroup, NestedJobProgressLink } from "../../../type/job";
import { MiniTaskProgressBar } from "../TaskProgressBar";

export type AdvancedProgressBarProps = {
  progressGroups: JobProgressGroup[] | undefined;
} & ClassNameProps &
  Pick<AdvancedProgressBarSegmentProps, "onClickLink">;

export const AdvancedProgressBar = ({
  progressGroups,
  className,
  ...segmentProps
}: AdvancedProgressBarProps) => {
  return (
    <Table className={className}>
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

const NameContainerTableCell = styled(TableCell)(({ theme }) => ({
  paddingLeft: 0,
  whiteSpace: "nowrap",
  display: "flex",
  alignItems: "center",
}));

const SpacerSpan = styled("span")(({ theme }) => ({
  width: 4,
}));

const ProgressBarContainerTableCell = styled(TableCell)(({ theme }) => ({
  width: "100%",
  paddingRight: 0,
}));

const StyledRiSubtractLine = styled(RiSubtractLine)(({ theme }) => ({
  width: 16,
  height: 16,
  verticalAlign: "top",
  marginRight: theme.spacing(0.5),
}));

const StyledButton = styled("button")(({ theme }) => ({
  border: "none",
  cursor: "pointer",
  color: "#036DCF",
  textDecoration: "underline",
  background: "none",
}));

const StyledLink = styled(Link)(({ theme }) => ({
  border: "none",
  cursor: "pointer",
  color: "#036DCF",
  textDecoration: "underline",
  background: "none",
}));

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

  const StyledIconComponent = styled(IconComponent)(({ theme }) => ({
    width: 16,
    height: 16,
    verticalAlign: "top",
    marginRight: theme.spacing(0.5),
  }));

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
          <NameContainerTableCell
            onClick={() => {
              setExpanded(!expanded);
            }}
          >
            {showParentCollapseButton && (
              <StyledRiSubtractLine
                title="Collapse group"
                onClick={onParentCollapseButtonPressed}
                style={{ marginLeft: 24 * (nestedIndex - 1) }}
              />
            )}
            <StyledIconComponent
              title={expanded ? "Collapse" : "Expand"}
              sx={[children.length === 0 && { visibility: "hidden" }]}
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
                <StyledButton
                  onClick={(event) => {
                    onClickLink?.(link);
                    event.stopPropagation();
                  }}
                >
                  {name}
                </StyledButton>
              ) : (
                <StyledLink to={`tasks/${link.id}`}>{name}</StyledLink>
              )
            ) : (
              name
            )}
            {isGroup && (
              <React.Fragment>
                <SpacerSpan />
                {"("}
                <RiCloseLine /> {children.length}
                {")"}
              </React.Fragment>
            )}
          </NameContainerTableCell>
          <ProgressBarContainerTableCell>
            <MiniTaskProgressBar {...progress} showTotal />
          </ProgressBarContainerTableCell>
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
