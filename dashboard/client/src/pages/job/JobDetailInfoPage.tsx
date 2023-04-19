import { makeStyles } from "@material-ui/core";
import React from "react";
import { DurationText } from "../../common/DurationText";
import { formatDateFromTimeMs } from "../../common/formatUtils";
import Loading from "../../components/Loading";
import { MetadataSection } from "../../components/MetadataSection";
import { StatusChip } from "../../components/StatusChip";
import TitleCard from "../../components/TitleCard";
import { MainNavPageInfo } from "../layout/mainNavContext";

import { useJobDetail } from "./hook/useJobDetail";

const useStyle = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(2),
  },
}));

export const JobDetailInfoPage = () => {
  // TODO(aguo): Add more content to this page!

  const classes = useStyle();
  const { job, msg, isLoading, params } = useJobDetail();

  if (!job) {
    return (
      <div className={classes.root}>
        <MainNavPageInfo
          pageInfo={{
            title: "Info",
            id: "job-info",
            path: undefined,
          }}
        />
        <Loading loading={isLoading} />
        <TitleCard title={`JOB - ${params.id}`}>
          <StatusChip type="job" status="LOADING" />
          <br />
          Request Status: {msg} <br />
        </TitleCard>
      </div>
    );
  }

  return (
    <div className={classes.root}>
      <MainNavPageInfo
        pageInfo={{
          title: "Info",
          id: "job-info",
          path: job.job_id ? `/jobs/${job.job_id}/info` : undefined,
        }}
      />
      <TitleCard title={`JOB - ${params.id}`}>
        <MetadataSection
          metadataList={[
            {
              label: "Entrypoint",
              content: job.entrypoint
                ? {
                    value: job.entrypoint,
                    copyableValue: job.entrypoint,
                  }
                : { value: "-" },
            },
            {
              label: "Status",
              content: <StatusChip type="job" status={job.status} />,
            },
            {
              label: "Job ID",
              content: job.job_id
                ? {
                    value: job.job_id,
                    copyableValue: job.job_id,
                  }
                : { value: "-" },
            },
            {
              label: "Submission ID",
              content: job.submission_id
                ? {
                    value: job.submission_id,
                    copyableValue: job.submission_id,
                  }
                : {
                    value: "-",
                  },
            },
            {
              label: "Duration",
              content: job.start_time ? (
                <DurationText
                  startTime={job.start_time}
                  endTime={job.end_time}
                />
              ) : (
                <React.Fragment>-</React.Fragment>
              ),
            },
            {
              label: "Started at",
              content: {
                value: job.start_time
                  ? formatDateFromTimeMs(job.start_time)
                  : "-",
              },
            },
            {
              label: "Ended at",
              content: {
                value: job.end_time ? formatDateFromTimeMs(job.end_time) : "-",
              },
            },
          ]}
        />
      </TitleCard>
    </div>
  );
};
