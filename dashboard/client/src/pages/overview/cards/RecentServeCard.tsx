import { createStyles, makeStyles } from "@material-ui/core";
import _ from "lodash";
import React from "react";
import { ServeStatusIcon } from "../../../common/ServeStatus";
import { ListItemCard } from "../../../components/ListItemCard";
import { useServeApplications } from "../../serve/hook/useServeApplications";

const useStyles = makeStyles((theme) =>
  createStyles({
    icon: {
      marginRight: theme.spacing(1),
    },
  }),
);

type RecentServeCardProps = {
  className?: string;
};

export const RecentServeCard = ({ className }: RecentServeCardProps) => {
  const classes = useStyles();

  // Use mock data by uncommenting the following line
  // const applications = mockServeApplications.applications;
  const { allServeApplications: applications } = useServeApplications();

  const sortedApplications = _.orderBy(
    applications,
    ["last_deployed_time_s"],
    ["desc"],
  ).slice(0, 6);

  const sortedApplicationsToRender = sortedApplications.map((app) => {
    return {
      title: app.name,
      subtitle: app?.deployed_app_config?.import_path || "-",
      link: app.name ? `/serve/applications/${app.name}` : undefined,
      className: className,
      icon: <ServeStatusIcon className={classes.icon} app={app} small />,
    };
  });

  return (
    <ListItemCard
      headerTitle="Serve Applications"
      className={className}
      items={sortedApplicationsToRender}
      emptyListText="No Applications yet..."
      footerText="View all applications"
      footerLink="/serve"
    />
  );
};
