import { Snackbar } from "@material-ui/core";
import { Alert } from "@material-ui/lab";
import React, { useState } from "react";
import { ActorGroup, ActorState } from "../../../api";
import { stableSort } from "../../../common/tableUtils";

import { sum } from "../../../common/util";
import ActorClassGroup from "./ActorClassGroup";

type ActorClassGroupsProps = {
  actorGroups: { [groupKey: string]: ActorGroup };
};

const ActorClassGroups: React.FC<ActorClassGroupsProps> = ({ actorGroups }) => {
  const numInfeasible = (group: ActorGroup) =>
    group.summary.stateToCount[ActorState.Infeasible] ?? 0;
  const totalInfeasible = sum(Object.values(actorGroups).map(numInfeasible));
  const [warningOpen, setWarningOpen] = useState(totalInfeasible > 0);
  const groupComparator = (
    [title1, group1]: [string, ActorGroup],
    [title2, group2]: [string, ActorGroup],
  ) => {
    const infeasible1 = numInfeasible(group1);
    const infeasible2 = numInfeasible(group2);
    if (infeasible1 !== infeasible2) {
      return infeasible1 > infeasible2 ? -1 : 1;
    }
    return title1 > title2 ? 1 : -1;
  };
  const children = stableSort(
    Object.entries(actorGroups),
    groupComparator,
  ).map(([title, actorGroup]) => (
    <ActorClassGroup
      actorGroup={actorGroup}
      title={title}
      key={`acg-${title}`}
    />
  ));

  return (
    <React.Fragment>
      <Snackbar open={warningOpen}>
        <Alert severity="warning" onClose={() => setWarningOpen(false)}>
          There are one or more actors that cannot currently be created due to
          insufficient cluster resources. These have been sorted to the top of
          the list. If you are using autoscaling functionality, you may ignore
          this message.
        </Alert>
      </Snackbar>
      {children}
    </React.Fragment>
  );
};

export default ActorClassGroups;
