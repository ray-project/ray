import React from "react";
import { RiInformationLine, RiLineChartLine } from "react-icons/ri";
import { SideTabLayout, SideTabRouteLink } from "../layout/SideTabLayout";

export const ClusterLayout = () => {
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
