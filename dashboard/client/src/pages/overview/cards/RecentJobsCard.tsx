import { createStyles, makeStyles } from "@material-ui/core";
import _ from "lodash";
import React from "react";
import { JobStatusIcon } from "../../../common/JobStatus";
import { ListItemCard } from "../../../components/ListItemCard";
import { UnifiedJob } from "../../../type/job";
import { useJobList } from "../../job/hook/useJobList";

const useStyles = makeStyles((theme) =>
  createStyles({
    icon: {
      marginRight: theme.spacing(1),
    },
  }),
);

type RecentJobsCardProps = {
  className?: string;
};

const getLink = (job: UnifiedJob) => {
  if (job.job_id !== null && job.job_id !== "") {
    return `/jobs/${job.job_id}`;
  } else if (job.submission_id !== null && job.submission_id !== "") {
    return `/jobs/${job.submission_id}`;
  }
  return undefined;
};

export const RecentJobsCard = ({ className }: RecentJobsCardProps) => {
  const classes = useStyles();

  const { jobList } = useJobList();

  const sortedJobs = _.orderBy(jobList, ["startTime"], ["desc"]).slice(0, 6);

  const sortedJobToRender = sortedJobs.map((job) => {
    return {
      title: job.job_id ?? job.submission_id ?? undefined,
      subtitle: job.entrypoint,
      link: getLink(job),
      className: className,
      icon: <JobStatusIcon className={classes.icon} job={job} />,
    };
  });

  return (
    <ListItemCard
      headerTitle="Recent jobs"
      className={className}
      items={sortedJobToRender}
      emptyListText="No jobs yet..."
      footerText="View all jobs"
      footerLink="/jobs"
    />
  );
};
