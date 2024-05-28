import { useState } from "react";
import useSWR from "swr";
import { API_REFRESH_INTERVAL_MS } from "../../../common/constants";
import { getServeApplications } from "../../../service/serve";
import { ServeSystemActorStatus } from "../../../type/serve";
import { ServeDetails } from "../ServeSystemDetails";

const SERVE_PROXY_STATUS_SORT_ORDER: Record<ServeSystemActorStatus, number> = {
  [ServeSystemActorStatus.UNHEALTHY]: 0,
  [ServeSystemActorStatus.STARTING]: 1,
  [ServeSystemActorStatus.HEALTHY]: 2,
  [ServeSystemActorStatus.DRAINING]: 3,
};

export const useServeDeployments = () => {
  const [page, setPage] = useState({ pageSize: 10, pageNo: 1 });

  const [proxiesPage, setProxiesPage] = useState({
    pageSize: 10,
    pageNo: 1,
  });

  const { data, error } = useSWR(
    "useServeDeployments",
    async () => {
      const rsp = await getServeApplications();

      if (rsp) {
        const serveApplicationsList = rsp.data
          ? Object.values(rsp.data.applications).sort(
              (a, b) =>
                (b.last_deployed_time_s ?? 0) - (a.last_deployed_time_s ?? 0),
            )
          : [];

        const serveDeploymentsList = serveApplicationsList.flatMap((app) =>
          Object.values(app.deployments).map((d) => ({
            ...d,
            applicationName: app.name,
            application: app,
          })),
        );
        return { ...rsp.data, serveApplicationsList, serveDeploymentsList };
      }
    },
    { refreshInterval: API_REFRESH_INTERVAL_MS },
  );

  const serveDetails: ServeDetails | undefined = data
    ? {
        http_options: data.http_options,
        grpc_options: data.grpc_options,
        proxy_location: data.proxy_location,
        controller_info: data.controller_info,
      }
    : undefined;

  const proxies =
    data && data.proxies
      ? Object.values(data.proxies).sort(
          (a, b) =>
            SERVE_PROXY_STATUS_SORT_ORDER[b.status] -
            SERVE_PROXY_STATUS_SORT_ORDER[a.status],
        )
      : [];

  const serveDeploymentsList = data?.serveDeploymentsList ?? [];
  const serveApplicationsList = data?.serveApplicationsList ?? [];

  return {
    serveDetails,
    serveDeployments: serveDeploymentsList,
    proxies,
    error,
    page,
    setPage: (key: string, val: number) => setPage({ ...page, [key]: val }),
    proxiesPage,
    setProxiesPage: (key: string, val: number) =>
      setProxiesPage({ ...proxiesPage, [key]: val }),
    serveApplications: serveApplicationsList,
  };
};

export const useServeApplicationDetails = (
  applicationName: string | undefined,
) => {
  const [page, setPage] = useState({ pageSize: 10, pageNo: 1 });
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
    allDeployments: deployments,
  };
};

export const useServeDeploymentDetails = (
  applicationName: string | undefined,
  deploymentName: string | undefined,
) => {
  const [page, setPage] = useState({ pageSize: 10, pageNo: 1 });
  const [filter, setFilter] = useState<
    {
      key: "replica_id" | "state";
      val: string;
    }[]
  >([]);
  const changeFilter = (key: "replica_id" | "state", val: string) => {
    const f = filter.find((e) => e.key === key);
    if (f) {
      f.val = val;
    } else {
      filter.push({ key, val });
    }
    setFilter([...filter]);
  };

  // TODO(aguo): Use a fetch by deploymentId endpoint?
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
  const deployment = deploymentName
    ? application?.deployments[deploymentName]
    : undefined;

  const replicas = deployment?.replicas ?? [];

  // Need to expose loading because it's not clear if undefined values
  // for application, deployment, or replica means loading or missing data.
  return {
    loading: !data && !error,
    application,
    deployment,
    filteredReplicas: replicas.filter((replica) =>
      filter.every((f) =>
        f.val ? replica[f.key] && (replica[f.key] ?? "").includes(f.val) : true,
      ),
    ),
    changeFilter,
    page,
    setPage: (key: string, val: number) => setPage({ ...page, [key]: val }),
    error,
  };
};

export const useServeReplicaDetails = (
  applicationName: string | undefined,
  deploymentName: string | undefined,
  replicaId: string | undefined,
) => {
  // TODO(aguo): Use a fetch by replicaId endpoint?
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

export const useServeProxyDetails = (proxyId: string | undefined) => {
  const { data, error, isLoading } = useSWR(
    "useServeApplications",
    async () => {
      const rsp = await getServeApplications();

      if (rsp) {
        return rsp.data;
      }
    },
    { refreshInterval: API_REFRESH_INTERVAL_MS },
  );

  const proxy = proxyId ? data?.proxies?.[proxyId] : undefined;

  // Need to expose loading because it's not clear if undefined values
  // for proxies means loading or missing data.
  return {
    loading: isLoading,
    proxy,
    error,
  };
};

export const useServeControllerDetails = () => {
  const { data, error, isLoading } = useSWR(
    "useServeApplications",
    async () => {
      const rsp = await getServeApplications();

      if (rsp) {
        return rsp.data;
      }
    },
    { refreshInterval: API_REFRESH_INTERVAL_MS },
  );

  // Need to expose loading because it's not clear if undefined values
  // for serve controller means loading or missing data.
  return {
    loading: isLoading,
    controller: data?.controller_info,
    error,
  };
};
