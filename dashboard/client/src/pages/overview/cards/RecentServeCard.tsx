import { styled } from "@mui/material/styles";
import _ from "lodash";
import React from "react";
import { ServeStatusIcon } from "../../../common/ServeStatus";
import { ListItemCard } from "../../../components/ListItemCard";
import { useServeDeployments } from "../../serve/hook/useServeApplications";

const StyledServeStatusIcon = styled(ServeStatusIcon)(({theme}) => ({ 
  marginRight: theme.spacing(1),
}));

type RecentServeCardProps = {
  className?: string;
};

export const RecentServeCard = ({ className }: RecentServeCardProps) => {
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
        <StyledServeStatusIcon deployment={deployment} small />
      ),
    };
  });

  return (
    <ListItemCard
      headerTitle="Serve Deployments"
      className={className}
      items={sortedDeploymentsToRender}
      emptyListText="No Deployments yet..."
      footerText="View all deployments"
      footerLink="/serve"
    />
  );
};
