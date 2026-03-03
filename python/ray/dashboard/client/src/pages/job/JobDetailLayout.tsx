import React from "react";
import {
  RiGradienterLine,
  RiInformationLine,
  RiLineChartLine,
} from "react-icons/ri";
import { Outlet } from "react-router-dom";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { SideTabLayout, SideTabRouteLink } from "../layout/SideTabLayout";
import { useJobDetail } from "./hook/useJobDetail";

export const JobPage = () => {
  const { job, params } = useJobDetail();

  const jobId = job?.job_id ?? job?.submission_id;
  const pageInfo = jobId
    ? {
        title: jobId ?? "Job",
        pageTitle: jobId ? `${jobId} | Job` : undefined,
        id: "job-detail",
        path: jobId,
      }
    : {
        title: "Job",
        id: "job-detail",
        path: params.id,
      };
  return (
    <div>
      <MainNavPageInfo pageInfo={pageInfo} />
      <Outlet />
    </div>
  );
};

export const JobDetailLayout = () => {
  return (
    <SideTabLayout>
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
