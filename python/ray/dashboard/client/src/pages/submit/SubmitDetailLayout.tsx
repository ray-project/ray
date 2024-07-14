import React from "react";
import { RiInformationLine, RiLineChartLine } from "react-icons/ri";
import { Outlet } from "react-router-dom";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { SideTabLayout, SideTabRouteLink } from "../layout/SideTabLayout";
import { useSubmitDetail } from "./hook/useSubmitDetail";

export const SubmitPage = () => {
  const { submit, params } = useSubmitDetail();
  const submitId = submit?.submission_id;
  const pageInfo = submitId
    ? {
        title: submitId ?? "Submit",
        pageTitle: submitId ? `${submitId} | Submit` : undefined,
        id: "submit-detail",
        path: submitId,
      }
    : {
        title: "Submit",
        id: "submit-detail",
        path: params.submitId,
      };
  return (
    <div>
      <MainNavPageInfo pageInfo={pageInfo} />
      <Outlet />
    </div>
  );
};

export const SubmitDetailLayout = () => {
  return (
    <SideTabLayout>
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
