import React from "react";
import {
  RiGradienterLine,
  RiInformationLine,
  RiLineChartLine,
} from "react-icons/ri";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { SideTabLayout, SideTabRouteLink } from "../layout/SideTabLayout";
import { useJobDetail } from "./hook/useJobDetail";

export const JobDetailLayout = () => {
  const { job } = useJobDetail();

  const jobId = job?.job_id ?? job?.submission_id;
  const pageInfo = job
    ? {
        title: jobId ?? "Job",
        pageTitle: jobId ? `${jobId} | Job` : undefined,
        id: "job-detail",
        path: jobId ? `/jobs/${jobId}` : undefined,
      }
    : {
        title: "Job",
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
      <SideTabRouteLink
        to="actors"
        tabId="actors"
        title="Actors"
        Icon={RiGradienterLine}
      />
    </SideTabLayout>
  );
};
