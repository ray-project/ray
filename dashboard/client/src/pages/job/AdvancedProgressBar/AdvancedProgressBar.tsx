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
import { JobProgressByLineage } from "../../../type/job";
import { useJobProgressByLineage } from "../hook/useJobProgress";
import { MiniTaskProgressBar } from "../TaskProgressBar";

export type AdvancedProgressBarProps = {
  jobId: string;
} & ClassNameProps;

export const AdvancedProgressBar = ({
  jobId,
  className,
}: AdvancedProgressBarProps) => {
  const { progress } = useJobProgressByLineage(jobId);
  return (
    <Table className={className}>
      {progress ? (
        progress.map((lineageProgress) => (
          <AdvancedProgressBarSegment
            key={lineageProgress.key}
            lineageProgress={lineageProgress}
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
    },
    progressBarContainer: {
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
  lineageProgress: JobProgressByLineage;
  /**
   * Whether the segment should be expanded or not.
   * Only applies to this segment and not it's children.
   */
  startExpanded?: boolean;
};

const AdvancedProgressBarSegment = ({
  lineageProgress: { name, lineage, progress, children },
  startExpanded = false,
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
              style={{ marginLeft: 24 * lineage.length }}
            />
          ) : (
            <RiArrowRightSLine
              className={classNames(classes.icon, {
                [classes.iconHidden]: children.length === 0,
              })}
              style={{ marginLeft: 24 * lineage.length }}
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
          <AdvancedProgressBarSegment key={child.key} lineageProgress={child} />
        ))}
    </React.Fragment>
  );
};
