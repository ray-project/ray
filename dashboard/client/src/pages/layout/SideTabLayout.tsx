import { createStyles, makeStyles } from "@material-ui/core";
import classNames from "classnames";
import React, { PropsWithChildren, useContext } from "react";
import { IconType } from "react-icons/lib";
import { Link, Outlet } from "react-router-dom";
import { StyledTooltip } from "../../components/Tooltip";
import {
  SideTabContext,
  useSideTabPage,
  useSideTabState,
} from "./sideTabContext";

const useStyles = makeStyles((theme) =>
  createStyles({
    root: {},
    tabsContainer: {
      position: "fixed",
      height: "100%",
      width: 64,
      display: "flex",
      flexDirection: "column",
      flexWrap: "nowrap",
      alignItems: "center",
      paddingTop: theme.spacing(1),
      paddingBottom: theme.spacing(2),
      background: "white",
      borderRight: "1px solid #D2DCE6",
    },
    contentContainer: {
      marginLeft: 64,
    },
  }),
);

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
  const classes = useStyles();

  const sideTabState = useSideTabState();
  return (
    <SideTabContext.Provider value={sideTabState}>
      <div className={classes.root}>
        <div className={classes.tabsContainer}>{children}</div>
        <div className={classes.contentContainer}>
          <Outlet />
        </div>
      </div>
    </SideTabContext.Provider>
  );
};

const useSideTabStyles = makeStyles((theme) =>
  createStyles({
    tab: {
      width: 40,
      height: 40,
      display: "flex",
      justifyContent: "center",
      alignItems: "center",
      color: "#5F6469",
      borderRadius: 4,
      marginTop: theme.spacing(1),
    },
    tabHighlighted: {
      backgroundColor: "#EBF3FB",
      color: "#036DCF",
    },
    icon: {
      width: 20,
      height: 20,
    },
  }),
);

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
  const classes = useSideTabStyles();
  const { selectedTab } = useContext(SideTabContext);
  const isSelected = selectedTab === tabId;
  return (
    <StyledTooltip title={title} placement="right">
      <div
        id={tabId}
        role="tab"
        aria-selected={isSelected}
        className={classNames(classes.tab, {
          [classes.tabHighlighted]: isSelected,
        })}
      >
        {Icon ? <Icon className={classes.icon} /> : tabId}
      </div>
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
