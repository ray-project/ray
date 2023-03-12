import { useContext, useState } from "react";
import useSWR from "swr";
import { GlobalContext } from "../../../App";
import { API_REFRESH_INTERVAL_MS } from "../../../common/constants";
import { getServeApplications } from "../../../service/serve";

export const useServeApplications = () => {
  const [page, setPage] = useState({ pageSize: 10, pageNo: 1 });
  const { ipLogMap } = useContext(GlobalContext);
  const [filter, setFilter] = useState<
    {
      key: "name" | "app_status";
      val: string;
    }[]
  >([]);
  const changeFilter = (key: "name" | "app_status", val: string) => {
    const f = filter.find((e) => e.key === key);
    if (f) {
      f.val = val;
    } else {
      filter.push({ key, val });
    }
    setFilter([...filter]);
  };

  const { data } = useSWR(
    "useServeApplications",
    async () => {
      const rsp = await getServeApplications();

      if (rsp) {
        return rsp.data;
      }
    },
    { refreshInterval: API_REFRESH_INTERVAL_MS },
  );

  const serveDetails = data ? { host: data.host, port: data.port } : undefined;
  const serveApplicationsList = data
    ? Object.values(data.application_details).sort(
        (a, b) => (b.deployment_timestamp ?? 0) - (a.deployment_timestamp ?? 0),
      )
    : [];

  return {
    serveDetails,
    serveApplicationsList: serveApplicationsList.filter((app) =>
      filter.every((f) =>
        f.val ? app[f.key] && (app[f.key] ?? "").includes(f.val) : true,
      ),
    ),
    changeFilter,
    page,
    setPage: (key: string, val: number) => setPage({ ...page, [key]: val }),
    ipLogMap,
    unfilteredList: serveApplicationsList,
  };
};

export const useServeApplicationDetails = (
  applicationName: string | undefined,
) => {
  const [page, setPage] = useState({ pageSize: 10, pageNo: 1 });
  const { ipLogMap } = useContext(GlobalContext);
  const [filter, setFilter] = useState<
    {
      key: "name" | "deployment_status";
      val: string;
    }[]
  >([]);
  const changeFilter = (key: "name" | "deployment_status", val: string) => {
    const f = filter.find((e) => e.key === key);
    if (f) {
      f.val = val;
    } else {
      filter.push({ key, val });
    }
    setFilter([...filter]);
  };

  // TODO(aguo): Use a fetch by applicationName endpoint?
  const { data } = useSWR(
    "useServeApplications",
    async () => {
      const rsp = await getServeApplications();

      if (rsp) {
        return rsp.data;
      }
    },
    { refreshInterval: API_REFRESH_INTERVAL_MS },
  );

  const application = applicationName
    ? data?.application_details?.[
        applicationName !== "-" ? applicationName : ""
      ]
    : undefined;
  const deployments = application
    ? Object.values(application.deployments_details).sort((a, b) =>
        a.name.localeCompare(b.name),
      )
    : [];

  return {
    application,
    deployments: deployments.filter((deployment) =>
      filter.every((f) =>
        f.val
          ? deployment[f.key] && (deployment[f.key] ?? "").includes(f.val)
          : true,
      ),
    ),
    changeFilter,
    page,
    setPage: (key: string, val: number) => setPage({ ...page, [key]: val }),
    ipLogMap,
    unfilteredList: deployments,
  };
};
