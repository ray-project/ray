import { Box } from "@mui/material";
import React, { PropsWithChildren, useContext } from "react";
import { IconType } from "react-icons/lib";
import { Link, Outlet } from "react-router-dom";
import { StyledTooltip } from "../../components/Tooltip";
import {
  SideTabContext,
  useSideTabPage,
  useSideTabState,
} from "./sideTabContext";

/**
 * A layout which has tabs on the left and content on the right.
 * Render <SideTab /> components as children to display tabs.
 *
 * The standard way to use this layout is to use nested routes.
 * A parent route should render the SideTabLayout with SideTabs
 * and children routes should render render a SideTabPage along
 * with the contents of the page.
 *
 * See "TestApp" in SideTabLayout.component.test.tsx for an example.
 */
export const SideTabLayout = ({ children }: PropsWithChildren<{}>) => {
  const sideTabState = useSideTabState();
  return (
    <SideTabContext.Provider value={sideTabState}>
      <Box>
        <Box
          sx={{
            position: "fixed",
            height: "100%",
            width: 64,
            display: "flex",
            flexDirection: "column",
            flexWrap: "nowrap",
            alignItems: "center",
            paddingTop: 1,
            paddingBottom: 2,
            background: "white",
            borderRight: "1px solid #D2DCE6",
          }}
        >
          {children}
        </Box>
        <Box sx={{ marginLeft: "64px" }}>
          <Outlet />
        </Box>
      </Box>
    </SideTabContext.Provider>
  );
};

export type SideTabProps = {
  /**
   * Required unique id to identify which tab should be highlighted when on that tab's page.
   */
  tabId: string;
  /**
   * The title is shown as a tooltip when hovering over the tab.
   */
  title: string;
  Icon?: IconType;
};

/**
 * A single tab to show in the tab bar
 */
export const SideTab = ({ tabId, title, Icon }: SideTabProps) => {
  const { selectedTab } = useContext(SideTabContext);
  const isSelected = selectedTab === tabId;
  return (
    <StyledTooltip title={title} placement="right">
      <Box
        id={tabId}
        role="tab"
        aria-selected={isSelected}
        sx={{
          width: 40,
          height: 40,
          display: "flex",
          justifyContent: "center",
          alignItems: "center",
          color: isSelected ? "#036DCF" : "#5F6469",
          backgroundColor: isSelected ? "#EBF3FB" : null,
          borderRadius: "4px",
          marginTop: 1,
          "&:hover": {
            backgroundColor: "#EBF3FB",
          },
        }}
      >
        {Icon ? <Box component={Icon} sx={{ width: 24, height: 24 }} /> : tabId}
      </Box>
    </StyledTooltip>
  );
};

export type SideTabRouteLinkProps = SideTabProps & {
  /**
   * Path to link to. If not provided, we default to tabId.
   */
  to?: string;
};

/**
 * A <SideTab /> that also links to a route. In most cases, the SideTabLayout will
 * be used closely with routing so we want the tabs to link to different page.
 */
export const SideTabRouteLink = ({ to, ...props }: SideTabRouteLinkProps) => {
  const { tabId } = props;
  return (
    <Link to={to ?? tabId}>
      <SideTab {...props} />
    </Link>
  );
};

export type SideTabPageProps = {
  tabId: string;
  children: JSX.Element | null;
};

/**
 * A page that represents the content of a tab. Render this when we are on the
 * tab's page and it will make sure the tab bar has the correct icon highlighted.
 */
export const SideTabPage = ({ tabId, children }: SideTabPageProps) => {
  useSideTabPage(tabId);
  return children;
};
