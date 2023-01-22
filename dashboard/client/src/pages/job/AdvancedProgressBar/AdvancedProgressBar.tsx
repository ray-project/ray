import {
  createStyles,
  makeStyles,
  Table,
  TableCell,
  TableRow,
} from "@material-ui/core";
import classNames from "classnames";
import React, { useState } from "react";
import { RiArrowDownSLine, RiArrowRightSLine } from "react-icons/ri";
import { ClassNameProps } from "../../../common/props";
import { JobProgressGroup } from "../../../type/job";
import { MiniTaskProgressBar } from "../TaskProgressBar";

export type AdvancedProgressBarProps = {
  progressGroups: JobProgressGroup[] | undefined;
} & ClassNameProps;

export const AdvancedProgressBar = ({
  progressGroups,
  className,
}: AdvancedProgressBarProps) => {
  return (
    <Table className={className}>
      {progressGroups !== undefined ? (
        progressGroups.map((group) => (
          <AdvancedProgressBarSegment
            key={group.key}
            jobProgressGroup={group}
            // By default expand the entire top level
            startExpanded
          />
        ))
      ) : (
        <TableRow>
          <TableCell>Loading...</TableCell>
        </TableRow>
      )}
    </Table>
  );
};

const useAdvancedProgressBarSegmentStyles = makeStyles((theme) =>
  createStyles({
    nameContainer: {
      paddingLeft: 0,
      whiteSpace: "nowrap",
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
};

const AdvancedProgressBarSegment = ({
  jobProgressGroup: { name, progress, children },
  startExpanded = false,
  nestedIndex = 1,
}: AdvancedProgressBarSegmentProps) => {
  const classes = useAdvancedProgressBarSegmentStyles();

  const [expanded, setExpanded] = useState(startExpanded);

  return (
    <React.Fragment>
      <TableRow>
        <TableCell
          className={classes.nameContainer}
          onClick={() => {
            setExpanded(!expanded);
          }}
        >
          {expanded ? (
            <RiArrowDownSLine
              className={classNames(classes.icon, {
                [classes.iconHidden]: children.length === 0,
              })}
              style={{ marginLeft: 24 * nestedIndex }}
            />
          ) : (
            <RiArrowRightSLine
              className={classNames(classes.icon, {
                [classes.iconHidden]: children.length === 0,
              })}
              style={{ marginLeft: 24 * nestedIndex }}
            />
          )}
          {name}
        </TableCell>
        <TableCell className={classes.progressBarContainer}>
          <MiniTaskProgressBar {...progress} showTotal />
        </TableCell>
      </TableRow>
      {expanded &&
        children.map((child) => (
          <AdvancedProgressBarSegment
            key={child.key}
            jobProgressGroup={child}
            nestedIndex={nestedIndex + 1}
          />
        ))}
    </React.Fragment>
  );
};
