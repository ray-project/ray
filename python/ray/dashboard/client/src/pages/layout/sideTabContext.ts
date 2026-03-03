import React, { useContext, useEffect, useState } from "react";

export type SideTabContextType = {
  selectedTab?: string;
  setSelectedTab: (value?: string) => void;
};

export const DEFAULT_VALUE: SideTabContextType = {
  setSelectedTab: () => {
    /* purposefully empty */
  },
};

export const SideTabContext =
  React.createContext<SideTabContextType>(DEFAULT_VALUE);

export const useSideTabState = (): SideTabContextType => {
  const [selectedTab, setSelectedTab] = useState<string>();

  return { selectedTab, setSelectedTab };
};

/**
 * Call this hook at the start of the component that represents the tab contents.
 */
export const useSideTabPage = (tabId: string) => {
  const { setSelectedTab } = useContext(SideTabContext);

  useEffect(() => {
    setSelectedTab(tabId);

    return () => {
      setSelectedTab(undefined);
    };
  }, [tabId, setSelectedTab]);
};
