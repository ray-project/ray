import React from "react";
import { Outlet } from "react-router-dom";
import { MainNavPageInfo } from "../layout/mainNavContext";

export const ActorLayout = () => {
  return (
    <div>
      <MainNavPageInfo
        pageInfo={{
          id: "actors",
          title: "Actors",
          path: "/actors",
        }}
      />
      <Outlet />
    </div>
  );
};
