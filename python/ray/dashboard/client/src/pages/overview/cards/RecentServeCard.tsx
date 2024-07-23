import { SxProps, Theme } from "@mui/material";
import _ from "lodash";
import React from "react";
import { ServeStatusIcon } from "../../../common/ServeStatus";
import { ListItemCard } from "../../../components/ListItemCard";
import { useServeDeployments } from "../../serve/hook/useServeApplications";

type RecentServeCardProps = {
  className?: string;
  sx?: SxProps<Theme>;
};

export const RecentServeCard = ({ className, sx }: RecentServeCardProps) => {
  const { serveDeployments: deployments } = useServeDeployments();

  const sortedDeployments = _.orderBy(
    deployments,
    ["application.last_deployed_time_s", "name"],
    ["desc", "asc"],
  ).slice(0, 6);

  const sortedDeploymentsToRender = sortedDeployments.map((deployment) => {
    return {
      title: deployment.name,
      subtitle:
        deployment.application.deployed_app_config?.import_path ||
        deployment.application.name ||
        deployment.application.route_prefix,
      link:
        deployment.application.name && deployment.name
          ? `/serve/applications/${encodeURIComponent(
              deployment.application.name,
            )}/${encodeURIComponent(deployment.name)}`
          : undefined,
      className: className,
      icon: (
        <ServeStatusIcon
          sx={{ marginRight: 1 }}
          deployment={deployment}
          small
        />
      ),
    };
  });

  return (
    <ListItemCard
      headerTitle="Serve Deployments"
      className={className}
      sx={sx}
      items={sortedDeploymentsToRender}
      emptyListText="No Deployments yet..."
      footerText="View all deployments"
      footerLink="/serve"
    />
  );
};
