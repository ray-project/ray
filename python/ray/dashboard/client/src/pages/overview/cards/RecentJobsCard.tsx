import { SxProps, Theme } from "@mui/material";
import _ from "lodash";
import React from "react";
import { JobStatusIcon } from "../../../common/JobStatus";
import { ListItemCard } from "../../../components/ListItemCard";
import { UnifiedJob } from "../../../type/job";
import { useJobList } from "../../job/hook/useJobList";

type RecentJobsCardProps = {
  className?: string;
  sx?: SxProps<Theme>;
};

const getLink = (job: UnifiedJob) => {
  if (job.job_id !== null && job.job_id !== "") {
    return `/jobs/${job.job_id}`;
  } else if (job.submission_id !== null && job.submission_id !== "") {
    return `/jobs/${job.submission_id}`;
  }
  return undefined;
};

export const RecentJobsCard = ({ className, sx }: RecentJobsCardProps) => {
  const { jobList } = useJobList();

  const sortedJobs = _.orderBy(jobList, ["startTime"], ["desc"]).slice(0, 6);

  const sortedJobToRender = sortedJobs.map((job) => {
    let title: string | undefined;
    if (job.submission_id && job.job_id) {
      title = `${job.submission_id} (${job.job_id})`;
    } else {
      title = job.submission_id ?? job.job_id ?? undefined;
    }
    return {
      title,
      subtitle: job.entrypoint,
      link: getLink(job),
      className: className,
      icon: <JobStatusIcon sx={{ marginRight: 1 }} job={job} />,
    };
  });

  return (
    <ListItemCard
      headerTitle="Recent jobs"
      className={className}
      sx={sx}
      items={sortedJobToRender}
      emptyListText="No jobs yet..."
      footerText="View all jobs"
      footerLink="/jobs"
    />
  );
};
