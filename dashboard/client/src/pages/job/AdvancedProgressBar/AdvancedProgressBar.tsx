import {
  createStyles,
  makeStyles,
  Table,
  TableBody,
  TableCell,
  TableRow,
} from "@material-ui/core";
import classNames from "classnames";
import React, { useState } from "react";
import {
  RiAddLine,
  RiArrowDownSLine,
  RiArrowRightSLine,
  RiCloseLine,
  RiSubtractLine,
} from "react-icons/ri";
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

const useAdvancedProgressBarSegmentStyles = makeStyles((theme) =>
  createStyles({
    nameContainer: {
      paddingLeft: 0,
      whiteSpace: "nowrap",
      display: "flex",
      alignItems: "center",
    },
    spacer: {
      width: 4,
    },
    progressBarContainer: {
      width: "100%",
      paddingRight: 0,
    },
    icon: {
      width: 16,
      height: 16,
      verticalAlign: "top",
      marginRight: theme.spacing(0.5),
    },
    iconHidden: {
      visibility: "hidden",
    },
    link: {
      border: "none",
      cursor: "pointer",
      color: "#036DCF",
      textDecoration: "underline",
      background: "none",
    },
  }),
);

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
  const classes = useAdvancedProgressBarSegmentStyles();

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
            className={classes.nameContainer}
            onClick={() => {
              setExpanded(!expanded);
            }}
          >
            {showParentCollapseButton && (
              <RiSubtractLine
                title="Collapse group"
                onClick={onParentCollapseButtonPressed}
                className={classNames(classes.icon)}
                style={{ marginLeft: 24 * (nestedIndex - 1) }}
              />
            )}
            <IconComponent
              title={expanded ? "Collapse" : "Expand"}
              className={classNames(classes.icon, {
                [classes.iconHidden]: children.length === 0,
              })}
              style={{
                // Complex logic on where to place the icon depending on the grouping type
                marginLeft: showParentCollapseButton
                  ? 4
                  : 24 * (isGroup ? nestedIndex - 1 : nestedIndex),
                marginRight: isGroup ? 28 : 4,
              }}
            />
            {link ? (
              <button
                className={classes.link}
                onClick={(event) => {
                  onClickLink?.(link);
                  event.stopPropagation();
                }}
              >
                {name}
              </button>
            ) : (
              name
            )}
            {isGroup && (
              <React.Fragment>
                <span className={classes.spacer} />
                {"("}
                <RiCloseLine /> {children.length}
                {")"}
              </React.Fragment>
            )}
          </TableCell>
          <TableCell className={classes.progressBarContainer}>
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
