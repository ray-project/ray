import { createStyles, makeStyles } from "@material-ui/core";
import classNames from "classnames";
import _ from "lodash";
import React from "react";
import { JobStatusIcon } from "../../../common/JobStatus";
import { CommonRecentCard } from "../../../components/ListItemCard";
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
      marginRight: theme.spacing(1),
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
    return {
      title: job.job_id ?? job.submission_id ?? undefined,
      subtitle: job.entrypoint,
      link:
        job.job_id !== null && job.job_id !== ""
          ? `?/jobs/${job.job_id}`
          : undefined,
      className: className,
      icon: <JobStatusIcon className={classes.icon} job={job} />,
    };
  });

  return (
    <CommonRecentCard
      headerTitle="Recent jobs"
      className={className}
      items={sortedJobToRender}
      emptyListText="No jobs yet..."
      footerText="View all jobs"
      footerLink="/jobs"
      icon={<JobStatusIcon className={classes.icon} job={job} />}
    ></CommonRecentCard>
  );
};
