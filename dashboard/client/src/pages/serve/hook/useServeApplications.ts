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
      key: "name" | "status";
      val: string;
    }[]
  >([]);
  const changeFilter = (key: "name" | "status", val: string) => {
    const f = filter.find((e) => e.key === key);
    if (f) {
      f.val = val;
    } else {
      filter.push({ key, val });
    }
    setFilter([...filter]);
  };

  const { data, error } = useSWR(
    "useServeApplications",
    async () => {
      const rsp = await getServeApplications();

      if (rsp) {
        return rsp.data;
      }
    },
    { refreshInterval: API_REFRESH_INTERVAL_MS },
  );

  const serveDetails = data
    ? { ...data.http_options, proxy_location: data.proxy_location }
    : undefined;
  const serveApplicationsList = data
    ? Object.values(data.applications).sort(
        (a, b) => (b.last_deployed_time_s ?? 0) - (a.last_deployed_time_s ?? 0),
      )
    : [];

  return {
    serveDetails,
    filteredServeApplications: serveApplicationsList.filter((app) =>
      filter.every((f) =>
        f.val ? app[f.key] && (app[f.key] ?? "").includes(f.val) : true,
      ),
    ),
    error,
    changeFilter,
    page,
    setPage: (key: string, val: number) => setPage({ ...page, [key]: val }),
    ipLogMap,
    allServeApplications: serveApplicationsList,
  };
};

export const useServeApplicationDetails = (
  applicationName: string | undefined,
) => {
  const [page, setPage] = useState({ pageSize: 10, pageNo: 1 });
  const { ipLogMap } = useContext(GlobalContext);
  const [filter, setFilter] = useState<
    {
      key: "name" | "status";
      val: string;
    }[]
  >([]);
  const changeFilter = (key: "name" | "status", val: string) => {
    const f = filter.find((e) => e.key === key);
    if (f) {
      f.val = val;
    } else {
      filter.push({ key, val });
    }
    setFilter([...filter]);
  };

  // TODO(aguo): Use a fetch by applicationName endpoint?
  const { data, error } = useSWR(
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
    ? data?.applications?.[applicationName !== "-" ? applicationName : ""]
    : undefined;
  const deployments = application
    ? Object.values(application.deployments).sort((a, b) =>
        a.name.localeCompare(b.name),
      )
    : [];

  // Need to expose loading because it's not clear if undefined values
  // for application means loading or missing data.
  return {
    loading: !data && !error,
    application,
    filteredDeployments: deployments.filter((deployment) =>
      filter.every((f) =>
        f.val
          ? deployment[f.key] && (deployment[f.key] ?? "").includes(f.val)
          : true,
      ),
    ),
    error,
    changeFilter,
    page,
    setPage: (key: string, val: number) => setPage({ ...page, [key]: val }),
    ipLogMap,
    allDeployments: deployments,
  };
};

export const useServeReplicaDetails = (
  applicationName: string | undefined,
  deploymentName: string | undefined,
  replicaId: string | undefined,
) => {
  // TODO(aguo): Use a fetch by replicaId endpoint?
  const { data, error } = useSWR(
    "useServeReplicaDetails",
    async () => {
      const rsp = await getServeApplications();

      if (rsp) {
        return rsp.data;
      }
    },
    { refreshInterval: API_REFRESH_INTERVAL_MS },
  );

  const application = applicationName
    ? data?.applications?.[applicationName !== "-" ? applicationName : ""]
    : undefined;
  const deployment = deploymentName
    ? application?.deployments[deploymentName]
    : undefined;
  const replica = deployment?.replicas.find(
    ({ replica_id }) => replica_id === replicaId,
  );

  // Need to expose loading because it's not clear if undefined values
  // for application, deployment, or replica means loading or missing data.
  return {
    loading: !data && !error,
    application,
    deployment,
    replica,
    error,
  };
};
