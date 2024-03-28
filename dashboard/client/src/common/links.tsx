import { Link } from "@mui/material";
import React, { PropsWithChildren } from "react";
import { Link as RouterLink } from "react-router-dom";
import { ClassNameProps } from "./props";

type ActorLinkProps = PropsWithChildren<
  {
    actorId: string;
    /**
     * This can be provided to override where we link to.
     */
    to?: string;
  } & ClassNameProps
>;

export const generateActorLink = (actorId: string) => `/actors/${actorId}`;

/**
 * A link to the top-level actors detail page.
 */
export const ActorLink = ({
  actorId,
  to,
  children,
  className,
}: ActorLinkProps) => {
  return (
    <Link
      className={className}
      component={RouterLink}
      to={to ?? generateActorLink(actorId)}
    >
      {children ?? actorId}
    </Link>
  );
};

type NodeLinkProps = PropsWithChildren<
  {
    nodeId: string;
    /**
     * This can be provided to override where we link to.
     */
    to?: string;
  } & ClassNameProps
>;

export const generateNodeLink = (nodeId: string) => `/cluster/nodes/${nodeId}`;

/**
 * A link to the top-level Cluster node detail page.
 */
export const NodeLink = ({
  nodeId,
  to,
  children,
  className,
}: NodeLinkProps) => {
  return (
    <Link
      className={className}
      component={RouterLink}
      to={to ?? generateNodeLink(nodeId)}
    >
      {children ?? nodeId}
    </Link>
  );
};
