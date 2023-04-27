import { createStyles, makeStyles } from "@material-ui/core";
import classNames from "classnames";
import _ from "lodash";
import React from "react";
import {
  RiCheckboxCircleFill,
  RiCloseCircleFill,
  RiLoader4Line,
} from "react-icons/ri";
import { CommonRecentCard } from "../../../components/ListItemCard";
import { UnifiedJob } from "../../../type/job";
import { useJobList } from "../../job/hook/useJobList";

const useStyles = makeStyles((theme) =>
  createStyles({
    root: {
      display: "flex",
      flexDirection: "column",
      padding: theme.spacing(2, 3),
    },
    listContainer: {
      marginTop: theme.spacing(2),
      flex: 1,
      overflow: "hidden",
    },
    listItem: {
      "&:not(:first-child)": {
        marginTop: theme.spacing(1),
      },
    },

    icon: {
      width: 24,
      height: 24,
      marginRight: theme.spacing(1),
      flex: "0 0 20px",
    },
    "@keyframes spinner": {
      from: {
        transform: "rotate(0deg)",
      },
      to: {
        transform: "rotate(360deg)",
      },
    },
    colorSuccess: {
      color: theme.palette.success.main,
    },
    colorError: {
      color: theme.palette.error.main,
    },
    iconRunning: {
      color: "#1E88E5",
      animationName: "$spinner",
      animationDuration: "1000ms",
      animationIterationCount: "infinite",
      animationTimingFunction: "linear",
    },
  }),
);

type RecentJobsCardProps = {
  className?: string;
};

export const RecentJobsCard = ({ className }: RecentJobsCardProps) => {
  const classes = useStyles();

  const { jobList } = useJobList();

  const sortedJobs = _.orderBy(jobList, ["startTime"], ["desc"]).slice(0, 6);

  const sortedJobToRender = sortedJobs.map((job) => {
    const icon = (() => {
      switch (job.status) {
        case "SUCCEEDED":
          return (
            <RiCheckboxCircleFill
              className={classNames(classes.icon, classes.colorSuccess)}
            />
          );
        case "FAILED":
        case "STOPPED":
          return (
            <RiCloseCircleFill
              className={classNames(classes.icon, classes.colorError)}
            />
          );
        default:
          return (
            <RiLoader4Line
              className={classNames(classes.icon, classes.iconRunning)}
            />
          );
      }
    })();

    return {
      title: job.job_id ?? job.submission_id,
      subtitle: job.entrypoint,
      link:
        job.job_id !== null && job.job_id !== ""
          ? `?/jobs/${job.job_id}`
          : undefined,
      className: className,
      icon: icon,
    };
  });

  return (
    <CommonRecentCard
      headerTitle={"Recent jobs"}
      className={className}
      items={sortedJobToRender}
      itemEmptyTip={"No jobs yet..."}
      footerText={"View all jobs"}
      footerLink={"/jobs"}
    ></CommonRecentCard>
  );
};
