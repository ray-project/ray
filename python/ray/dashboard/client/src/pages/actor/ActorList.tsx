import React from "react";
import ActorTable, { ActorTableProps } from "../../components/ActorTable";
import { ActorDetail } from "../../type/actor";
import { useActorList } from "./hook/useActorList";

/**
 * Represent the embedable actors page.
 */
const ActorList = ({
  jobId = null,
  detailPathPrefix = "",
  ...actorTableProps
}: {
  jobId?: string | null;
  detailPathPrefix?: string;
} & Pick<ActorTableProps, "filterToActorId" | "onFilterChange">) => {
  const data = useActorList();
  const actors: { [actorId: string]: ActorDetail } = data?.actors ? data.actors : {};
  const accelerators: {columns:{label:string; helpInfo?:string | undefined}[];}[] = data?.accelerators ? data?.accelerators : [];
  return (
    <div>
      <ActorTable
        actors={actors}
        accelerators={accelerators}
        jobId={jobId}
        detailPathPrefix={detailPathPrefix}
        {...actorTableProps}
      />
    </div>
  );
};

export default ActorList;
