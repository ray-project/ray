import React from "react";
import ActorTable, { ActorTableProps } from "../../components/ActorTable";
import { Actor } from "../../type/actor";
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
  const data: { [actorId: string]: Actor } | undefined = useActorList();
  const actors: { [actorId: string]: Actor } = data ? data : {};

  return (
    <div>
      <ActorTable
        actors={actors}
        jobId={jobId}
        detailPathPrefix={detailPathPrefix}
        {...actorTableProps}
      />
    </div>
  );
};

export default ActorList;
