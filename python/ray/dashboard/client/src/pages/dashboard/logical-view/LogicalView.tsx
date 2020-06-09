import {
  FormControl,
  FormHelperText,
  Input,
  InputLabel,
  Typography,
} from "@material-ui/core";
import React, { useState } from "react";
import { connect } from "react-redux";
import { RayletActorInfo, RayletInfoResponse } from "../../../api";
import { StoreState } from "../../../store";
import Actors from "./Actors";

const mapStateToProps = (state: StoreState) => ({
  rayletInfo: state.dashboard.rayletInfo,
});

const filterObj = (obj: Object, filterFn: any) =>
  Object.fromEntries(Object.entries(obj).filter(filterFn));

type LogicalViewProps = {
  rayletInfo: RayletInfoResponse | null;
} & ReturnType<typeof mapStateToProps>;

const LogicalView = ({ rayletInfo }: LogicalViewProps) => {
  const [nameFilter, setNameFilter] = useState("");

  if (rayletInfo === null) {
    return <Typography color="textSecondary">Loading...</Typography>;
  }
  let filteredActors = rayletInfo.actors;
  if (nameFilter !== "") {
    filteredActors = filterObj(
      filteredActors,
      ([_, actor]: [any, RayletActorInfo]) =>
        actorMatchesSearch(actor, nameFilter),
    );
  }
  return (
    <div>
      {Object.entries(rayletInfo.actors).length === 0 ? (
        <Typography color="textSecondary">No actors found.</Typography>
      ) : (
        <div>
          <FormControl>
            <InputLabel htmlFor="actor-name-filter">Actor Search</InputLabel>
            <Input
              id="actor-name-filter"
              aria-describedby="actor-name-helper-text"
              value={nameFilter}
              onChange={(event) => setNameFilter(event.target.value)}
            />
            <FormHelperText id="actor-name-helper-text">
              Search for an actor by name
            </FormHelperText>
          </FormControl>
          <Actors actors={filteredActors} />
        </div>
      )}
    </div>
  );
};

const actorMatchesSearch = (
  actor: RayletActorInfo,
  nameFilter: string,
): boolean => {
  const actorTitles = getNestedActorTitles(actor);
  const match = actorTitles.find(
    (actorTitle) => actorTitle.search(nameFilter) !== -1,
  );
  return match !== undefined;
};

const getNestedActorTitles = (actor: RayletActorInfo): string[] => {
  const actorTitle = actor.actorTitle;
  const titles: string[] = actorTitle ? [actorTitle] : [];
  if (actor.state === -1) {
    return titles;
  }
  const children = actor["children"];
  if (children === undefined || Object.entries(children).length === 0) {
    return titles;
  }
  const childrenTitles = Object.values(children).flatMap((actor) =>
    getNestedActorTitles(actor),
  );
  return titles.concat(childrenTitles);
};

export default connect(mapStateToProps)(LogicalView);
