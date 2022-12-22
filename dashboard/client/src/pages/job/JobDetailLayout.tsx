import React from "react";
import { RiInformationLine, RiLineChartLine } from "react-icons/ri";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { SideTabLayout, SideTabRouteLink } from "../layout/SideTabLayout";
import { useJobDetail } from "./hook/useJobDetail";

export const JobDetailLayout = () => {
  const { job } = useJobDetail();

  const pageInfo = job
    ? {
        title: job.job_id ?? "Job details",
        id: "job-detail",
        path: job.job_id ? `/new/jobs/${job.job_id}` : undefined,
      }
    : {
        title: "Job details",
        id: "job-detail",
        path: undefined,
      };

  return (
    <SideTabLayout>
      <MainNavPageInfo pageInfo={pageInfo} />
      <SideTabRouteLink tabId="info" title="Info" Icon={RiInformationLine} />
      <SideTabRouteLink
        to=""
        tabId="charts"
        title="Charts"
        Icon={RiLineChartLine}
      />
    </SideTabLayout>
  );
};
