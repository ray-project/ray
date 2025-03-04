import { ServeApplicationStatus } from "../../type/serve";

export const mockServeApplications = {
  applications: {
    app1: {
      name: "app1",
      route_prefix: "/app1",
      message: null,
      status: ServeApplicationStatus.RUNNING,
      deployed_app_config: {
        import_path: "app1:graph",
      },
      last_deployed_time_s: new Date().getTime() / 1000,
    },
    app2: {
      name: "app2",
      route_prefix: "/app2",
      message: null,
      status: ServeApplicationStatus.RUNNING,
      deployed_app_config: null,
      last_deployed_time_s: new Date().getTime() / 1000,
      deployments: {},
    },
    app3: {
      name: "app3",
      route_prefix: "/app3",
      message: null,
      status: ServeApplicationStatus.DEPLOYING,
      deployed_app_config: null,
      last_deployed_time_s: new Date().getTime() / 1000,
      deployments: {},
    },
    app4: {
      name: "app4",
      route_prefix: "/app4",
      message: null,
      status: ServeApplicationStatus.RUNNING,
      deployed_app_config: {
        import_path: "app4:graph",
      },
      last_deployed_time_s: new Date().getTime() / 1000,
    },
    app5: {
      name: "app5",
      route_prefix: "/app5",
      message: null,
      status: ServeApplicationStatus.DEPLOY_FAILED,
      deployed_app_config: {
        import_path: "app5:graph",
      },
      last_deployed_time_s: new Date().getTime() / 1000,
    },
    app6: {
      name: "app6",
      route_prefix: "/app6",
      message: null,
      status: ServeApplicationStatus.DELETING,
      deployed_app_config: null,
      last_deployed_time_s: new Date().getTime() / 1000,
      deployments: {},
    },
  },
};
