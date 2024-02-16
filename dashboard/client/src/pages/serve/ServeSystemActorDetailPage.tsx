import { createStyles, makeStyles, Typography } from "@material-ui/core";
import React from "react";
import { useParams } from "react-router-dom";
import { CollapsibleSection } from "../../common/CollapsibleSection";
import { generateActorLink, generateNodeLink } from "../../common/links";
import {
  MultiTabLogViewer,
  MultiTabLogViewerTabDetails,
} from "../../common/MultiTabLogViewer";
import { Section } from "../../common/Section";
import Loading from "../../components/Loading";
import { MetadataSection } from "../../components/MetadataSection";
import { StatusChip } from "../../components/StatusChip";
import { ActorDetail, ActorEnum } from "../../type/actor";
import {
  ServeProxy,
  ServeSystemActor,
  ServeSystemActorStatus,
} from "../../type/serve";
import { useFetchActor } from "../actor/hook/useActorDetail";
import { MainNavPageInfo } from "../layout/mainNavContext";
import {
  useServeControllerDetails,
  useServeProxyDetails,
} from "./hook/useServeApplications";

const useStyles = makeStyles((theme) =>
  createStyles({
    root: {
      padding: theme.spacing(3),
    },
  }),
);

export const ServeProxyDetailPage = () => {
  const classes = useStyles();
  const { proxyId } = useParams();

  const { proxy, loading } = useServeProxyDetails(proxyId);

  if (loading) {
    return <Loading loading />;
  }

  if (!proxy) {
    return (
      <Typography color="error">
        ProxyActor with id "{proxyId}" not found.
      </Typography>
    );
  }

  return (
    <div className={classes.root}>
      <MainNavPageInfo
        pageInfo={
          proxy.node_id
            ? {
                id: "serveProxy",
                title: `ProxyActor:${proxy.node_id}`,
                pageTitle: `${proxy.node_id} | Serve ProxyActor`,
                path: `/serve/proxies/${encodeURIComponent(proxy.node_id)}`,
              }
            : {
                id: "serveProxy",
                title: "ProxyActor",
                path: undefined,
              }
        }
      />
      <ServeSystemActorDetail actor={{ type: "proxy", detail: proxy }} />
    </div>
  );
};

export const ServeControllerDetailPage = () => {
  const classes = useStyles();
  const { controller, loading } = useServeControllerDetails();

  if (loading) {
    return <Loading loading />;
  }

  if (!controller) {
    return <Typography color="error">Serve controller not found.</Typography>;
  }

  return (
    <div className={classes.root}>
      <MainNavPageInfo
        pageInfo={{
          id: "serveController",
          title: "Serve Controller",
          path: "/serve/controller",
        }}
      />
      <ServeSystemActorDetail
        actor={{ type: "controller", detail: controller }}
      />
    </div>
  );
};

type ActorInfo =
  | {
      type: "proxy";
      detail: ServeProxy;
    }
  | {
      type: "controller";
      detail: ServeSystemActor;
    };

type ServeSystemActorDetailProps = {
  actor: ActorInfo;
};

export const convertActorStateForServeController = (
  actorState: ActorEnum | string,
) => {
  if (actorState === ActorEnum.ALIVE) {
    return ServeSystemActorStatus.HEALTHY;
  } else if (actorState === ActorEnum.DEAD) {
    return ServeSystemActorStatus.UNHEALTHY;
  } else {
    return ServeSystemActorStatus.STARTING;
  }
};

export const ServeSystemActorDetail = ({
  actor,
}: ServeSystemActorDetailProps) => {
  const name =
    actor.type === "proxy"
      ? `ProxyActor:${actor.detail.actor_id}`
      : "Serve Controller";

  const { data: fetchedActor } = useFetchActor(actor.detail.actor_id);

  return (
    <div>
      <MetadataSection
        metadataList={[
          {
            label: "Name",
            content: {
              value: name,
            },
          },
          {
            label: "Status",
            content:
              actor.type === "proxy" ? (
                <StatusChip type="serveProxy" status={actor.detail.status} />
              ) : fetchedActor ? (
                <StatusChip
                  type="serveController"
                  status={convertActorStateForServeController(
                    fetchedActor.state,
                  )}
                />
              ) : (
                {
                  value: "-",
                }
              ),
          },
          {
            label: "Actor ID",
            content: actor.detail.actor_id
              ? {
                  value: actor.detail.actor_id,
                  copyableValue: actor.detail.actor_id,
                  link: actor.detail.actor_id
                    ? generateActorLink(actor.detail.actor_id)
                    : undefined,
                }
              : {
                  value: "-",
                },
          },
          {
            label: "Actor name",
            content: {
              value: actor.detail.actor_name ? actor.detail.actor_name : "-",
            },
          },
          {
            label: "Worker ID",
            content: actor.detail.worker_id
              ? {
                  value: actor.detail.worker_id,
                  copyableValue: actor.detail.worker_id,
                }
              : {
                  value: "-",
                },
          },
          {
            label: "Node ID",
            content: actor.detail.node_id
              ? {
                  value: actor.detail.node_id,
                  copyableValue: actor.detail.node_id,
                  link: actor.detail.node_id
                    ? generateNodeLink(actor.detail.node_id)
                    : undefined,
                }
              : {
                  value: "-",
                },
          },
          {
            label: "Node IP",
            content: {
              value: actor.detail.node_ip ? actor.detail.node_ip : "-",
            },
          },
        ]}
      />
      {fetchedActor && actor.detail.log_file_path && (
        <CollapsibleSection title="Logs" startExpanded>
          <Section noTopPadding>
            <ServeSystemActorLogs
              type={actor.type}
              actor={fetchedActor}
              systemLogFilePath={actor.detail.log_file_path}
            />
          </Section>
        </CollapsibleSection>
      )}
    </div>
  );
};

export const LOG_CONTEXT_KEY_SERVE_CONTROLLER = "serve-entity-controllers";
export const LOG_CONTEXT_KEY_SERVE_PROXY = "serve-entity-proxies";

type ServeSystemActorLogsProps = {
  type: "controller" | "proxy";
  actor: Pick<ActorDetail, "address" | "actorId" | "pid">;
  systemLogFilePath: string;
};

const ServeSystemActorLogs = ({
  type,
  actor: {
    actorId,
    pid,
    address: { workerId, rayletId },
  },
  systemLogFilePath,
}: ServeSystemActorLogsProps) => {
  const tabs: MultiTabLogViewerTabDetails[] = [
    {
      title: type === "controller" ? "Controller logs" : "proxy logs",
      nodeId: rayletId,
      filename: systemLogFilePath.startsWith("/")
        ? systemLogFilePath.substring(1)
        : systemLogFilePath,
    },
  ];
  const contextKey =
    type === "controller"
      ? LOG_CONTEXT_KEY_SERVE_CONTROLLER
      : LOG_CONTEXT_KEY_SERVE_PROXY;
  return <MultiTabLogViewer tabs={tabs} contextKey={contextKey} />;
};
