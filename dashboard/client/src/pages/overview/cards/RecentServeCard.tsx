import { createStyles, makeStyles, Typography } from "@material-ui/core";
import classNames from "classnames";
import _ from "lodash";
import React from "react";
import {
  RiCheckboxCircleFill,
  RiCloseCircleFill,
  RiLoader4Line,
} from "react-icons/ri";
import { Link } from "react-router-dom";
import { UnifiedJob } from "../../../type/job";
import { ServeApplication, ServeApplicationStatus } from "../../../type/serve";
import { useServeApplications } from "../../serve/hook/useServeApplications";
import { LinkWithArrow, OverviewCard } from "./OverviewCard";

const useStyles = makeStyles((theme) =>
  createStyles({
    root: {
      display: "flex",
      flexDirection: "column",
      padding: theme.spacing(2, 3),
    },
    listContainer: {
      marginTop: theme.spacing(2),
      flex: 1,
      overflow: "hidden",
    },
    listItem: {
      "&:not(:first-child)": {
        marginTop: theme.spacing(1),
      },
    },
  }),
);

type RecentServeCardProps = {
  className?: string;
};

/*
{
    "": {
        "name": "",
        "route_prefix": "/test",
        "docs_path": null,
        "status": "RUNNING",
        "message": "",
        "last_deployed_time_s": 1682029771.0748637,
        "deployed_app_config": null,
        "deployments": {
            "MyModelDeployment": {
                "name": "MyModelDeployment",
                "status": "HEALTHY",
                "message": "",
                "deployment_config": {
                    "name": "MyModelDeployment",
                    "num_replicas": 1,
                    "max_concurrent_queries": 100,
                    "user_config": null,
                    "autoscaling_config": null,
                    "graceful_shutdown_wait_loop_s": 2,
                    "graceful_shutdown_timeout_s": 20,
                    "health_check_period_s": 10,
                    "health_check_timeout_s": 30,
                    "ray_actor_options": {
                        "runtime_env": {},
                        "num_cpus": 1
                    },
                    "is_driver_deployment": false
                },
                "replicas": [
                    {
                        "replica_id": "MyModelDeployment#oJRaQg",
                        "state": "RUNNING",
                        "pid": 364224,
                        "actor_name": "SERVE_REPLICA::MyModelDeployment#oJRaQg",
                        "actor_id": "b8c9082697cd69c16109eeb804000000",
                        "node_id": "3434841e491012452165c643fea4919d80d078554059d3e008d51713",
                        "node_ip": "172.31.5.171",
                        "start_time_s": 1682029903.403788
                    }
                ]
            }
        }
    }
}

*/

/*
  Design: 
    Header -> Title

    Body ->
    CardItem
    itemsCount
    styles

    Footer -> <LinkWithArrow>
*/
export const RecentServeCard = ({ className }: RecentServeCardProps) => {
  const classes = useStyles();

  const { allServeApplications: applications } = useServeApplications();

  const sortedApplications = _.orderBy(
    applications,
    ["last_deployed_time_s"],
    ["desc"],
  ).slice(0, 6);

  return (
    <OverviewCard className={classNames(classes.root, className)}>
      <Typography variant="h3">Recent Applications</Typography>
      <div className={classes.listContainer}>
        {sortedApplications.map((app) => (
          <RecentApplicationListItem
            key={app.name}
            className={classes.listItem}
            app={app}
          />
        ))}
        {sortedApplications.length === 0 && (
          <Typography variant="h4">No Applications yet...</Typography>
        )}
      </div>
      <LinkWithArrow text="View all applications" to="/serve" />
    </OverviewCard>
  );
};

const useRecentApplicationListItemStyles = makeStyles((theme) =>
  createStyles({
    root: {
      display: "flex",
      flexDirection: "row",
      flexWrap: "nowrap",
      alignItems: "center",
      textDecoration: "none",
    },
    icon: {
      width: 24,
      height: 24,
      marginRight: theme.spacing(1),
      flex: "0 0 20px",
    },
    "@keyframes spinner": {
      from: {
        transform: "rotate(0deg)",
      },
      to: {
        transform: "rotate(360deg)",
      },
    },
    colorSuccess: {
      color: theme.palette.success.main,
    },
    colorError: {
      color: theme.palette.error.main,
    },
    iconRunning: {
      color: "#1E88E5",
      animationName: "$spinner",
      animationDuration: "1000ms",
      animationIterationCount: "infinite",
      animationTimingFunction: "linear",
    },
    textContainer: {
      flex: "1 1 auto",
      width: `calc(100% - ${theme.spacing(1) + 20}px)`,
    },
    title: {
      color: "#036DCF",
    },
    entrypoint: {
      overflow: "hidden",
      textOverflow: "ellipsis",
      whiteSpace: "nowrap",
      color: "#5F6469",
    },
  }),
);

type RecentApplicationListItemProps = {
  className: string;
  app: Pick<ServeApplication, "name" | "route_prefix" | "status">;
};

const RecentApplicationListItem = ({
  app,
  className,
}: RecentApplicationListItemProps) => {
  const classes = useRecentApplicationListItemStyles();

  const icon = (() => {
    switch (app.status) {
      case ServeApplicationStatus.NOT_STARTED:
        return (
          <RiCheckboxCircleFill
            className={classNames(classes.icon, classes.colorSuccess)}
          />
        );
      case ServeApplicationStatus.DEPLOY_FAILED:
        return (
          <RiCloseCircleFill
            className={classNames(classes.icon, classes.colorError)}
          />
        );
      default:
        return (
          <RiLoader4Line
            className={classNames(classes.icon, classes.iconRunning)}
          />
        );
    }
  })();
  const cardContent = (
    <React.Fragment>
      {icon}
      <div className={classes.textContainer}>
        <Typography className={classes.title} variant="body2">
          {app.name}
        </Typography>
        <Typography
          className={classes.entrypoint}
          title={app.route_prefix}
          variant="caption"
        >
          {app.route_prefix}
        </Typography>
      </div>
    </React.Fragment>
  );
  return (
    <div className={className}>
      {app.name !== null ? (
        <Link className={classes.root} to={`/serve/applications/${app.name}`}>
          {cardContent}
        </Link>
      ) : (
        <div className={classes.root}>{cardContent}</div>
      )}
    </div>
  );
};
