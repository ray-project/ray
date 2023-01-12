import React, { useCallback, useContext, useEffect, useState } from "react";

export type MainNavPage = {
  /**
   * This gets shown in the breadcrumbs
   */
  title: string;
  /**
   * This helps identifies the current page a user is on and highlights the nav bar correctly.
   * This should be unique per page.
   * DO NOT change the pageId of a page. The behavior of the main nav and
   * breadcrumbs is undefined in that case.
   */
  id: string;
  /**
   * URL to link to access this route.
   */
  path?: string;
};

export type MainNavContextType = {
  mainNavPageHierarchy: MainNavPage[];
  addPage: (pageId: string) => void;
  updatePage: (title: string, id: string, path?: string) => void;
  removePage: (pageId: string) => void;
};

export const DEFAULT_VALUE: MainNavContextType = {
  mainNavPageHierarchy: [],
  addPage: () => {
    /* purposefully empty */
  },
  updatePage: () => {
    /* purposefully empty */
  },
  removePage: () => {
    /* purposefully empty */
  },
};

export const MainNavContext =
  React.createContext<MainNavContextType>(DEFAULT_VALUE);

export const useMainNavState = (): MainNavContextType => {
  const [pageHierarchy, setPageHierarchy] = useState<MainNavPage[]>([]);

  const addPage = useCallback((pageId) => {
    setPageHierarchy((hierarchy) => [
      ...hierarchy,
      { title: pageId, id: pageId },
    ]);
  }, []);

  const updatePage = useCallback((title, id, path) => {
    setPageHierarchy((hierarchy) => {
      const pageIndex = hierarchy.findIndex((page) => page.id === id);
      return [
        ...hierarchy.slice(0, pageIndex),
        { title, id, path },
        ...hierarchy.slice(pageIndex + 1),
      ];
    });
  }, []);

  const removePage = useCallback((pageId) => {
    setPageHierarchy((hierarchy) => {
      console.assert(
        hierarchy.length > 0,
        "Trying to remove when nav hierarchy length is 0",
      );
      const pageIndex = hierarchy.findIndex((page) => page.id === pageId);
      console.assert(
        pageIndex !== -1,
        "Trying to remove page that is not in the hiearchy...",
      );
      if (pageIndex === -1) {
        return hierarchy;
      }
      return [
        ...hierarchy.slice(0, pageIndex),
        ...hierarchy.slice(pageIndex + 1),
      ];
    });
  }, []);

  return {
    mainNavPageHierarchy: pageHierarchy,
    addPage,
    updatePage,
    removePage,
  };
};

const useMainNavPage = (pageInfo: MainNavPage) => {
  const { addPage, updatePage, removePage } = useContext(MainNavContext);

  // First effect to add and remove the page into the right spot of the hierarchy
  useEffect(() => {
    addPage(pageInfo.id);

    return () => {
      removePage(pageInfo.id);
    };
  }, [pageInfo.id, addPage, removePage]);

  // Second effect to allow for the title of a page to change over.
  useEffect(() => {
    updatePage(pageInfo.title, pageInfo.id, pageInfo.path);
    document.title = pageInfo.title;
    return () => {
      document.title = "Ray Dashboard";
    };
  }, [pageInfo.title, pageInfo.id, pageInfo.path, updatePage]);
};

type MainNavPageProps = {
  pageInfo: MainNavPage;
};

/**
 * Render this component at the top of your page if your page should belong in the Main Nav hierarchy
 * Users cannot call the hook directly because useEffect hooks are called from child -> parent on first render
 * However, children are always rendered in order so we can use a child component to force the parent
 * effect to run before the children effect.
 */
export const MainNavPageInfo = ({ pageInfo }: MainNavPageProps) => {
  useMainNavPage(pageInfo);
  return null;
};
