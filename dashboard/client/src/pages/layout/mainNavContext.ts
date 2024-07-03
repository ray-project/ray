import React, { useCallback, useContext, useEffect, useState } from "react";

export type MainNavPage = {
  /**
   * This gets shown in the breadcrumbs.
   */
  title: string;
  /**
   * This gets shown as the HTML document page title. If this is not set, the title field will be used automatically.
   */
  pageTitle?: string;
  /**
   * This helps identifies the current page a user is on and highlights the nav bar correctly.
   * This should be unique per page within an hiearchy. i.e. you should NOT put two pages with the same ID
   * as parents or children of each other.
   * DO NOT change the pageId of a page. The behavior of the main nav and
   * breadcrumbs is undefined in that case.
   */
  id: string;
  /**
   * URL to link to access this route.
   * If this begins with a `/`, it is treated as an absolute path.
   * If not, this is treated as a relative path and the path is appended to the parent breadcrumb's path.
   */
  path?: string;
};

export type MainNavContextType = {
  mainNavPageHierarchy: MainNavPage[];
  addPage: (pageId: string) => void;
  updatePage: (
    title: string,
    id: string,
    path?: string,
    pageTitle?: string,
  ) => void;
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

  const addPage = useCallback((pageId: string) => {
    setPageHierarchy((hierarchy) => [
      ...hierarchy,
      // Use dummy values for title and pageTitle to start. This gets filled by the next callback.
      { title: pageId, pageTitle: pageId, id: pageId },
    ]);
  }, []);

  const updatePage = useCallback(
    (
      title: string,
      id: string,
      path: string | undefined,
      pageTitle: string | undefined,
    ) => {
      setPageHierarchy((hierarchy) => {
        const pageIndex = hierarchy.findIndex((page) => page.id === id);
        return [
          ...hierarchy.slice(0, pageIndex),
          { title, pageTitle, id, path },
          ...hierarchy.slice(pageIndex + 1),
        ];
      });
    },
    [],
  );

  const removePage = useCallback((pageId: string) => {
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

  // Second effect to allow for the title of a page to change.
  useEffect(() => {
    updatePage(pageInfo.title, pageInfo.id, pageInfo.path, pageInfo.pageTitle);
    document.title = `${pageInfo.pageTitle ?? pageInfo.title} | Ray Dashboard`;
    return () => {
      document.title = "Ray Dashboard";
    };
  }, [
    pageInfo.title,
    pageInfo.id,
    pageInfo.path,
    pageInfo.pageTitle,
    updatePage,
  ]);
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
