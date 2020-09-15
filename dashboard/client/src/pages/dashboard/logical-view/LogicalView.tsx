import {
  Box,
  createStyles,
  FormControl,
  FormHelperText,
  Input,
  InputLabel,
  makeStyles,
  Theme,
  Typography,
} from "@material-ui/core";
import React, { useState } from "react";
import { useSelector } from "react-redux";
import { useDebounce } from "use-debounce";
import { StoreState } from "../../../store";
import ActorClassGroups from "./ActorClassGroups";

const useLogicalViewStyles = makeStyles((theme: Theme) =>
  createStyles({
    container: {
      marginBottom: theme.spacing(1),
    },
  }),
);

const actorClassMatchesSearch = (
  actorClass: string,
  nameFilter: string,
): boolean => {
  const loweredNameFilter = nameFilter.toLowerCase();
  return actorClass.toLowerCase().search(loweredNameFilter) !== -1;
};

const rayletInfoSelector = (state: StoreState) => state.dashboard.rayletInfo;

const LogicalView: React.FC = () => {
  const [nameFilter, setNameFilter] = useState("");
  const [debouncedNameFilter] = useDebounce(nameFilter, 500);
  const classes = useLogicalViewStyles();
  const rayletInfo = useSelector(rayletInfoSelector);
  if (rayletInfo === null || !rayletInfo.actorGroups) {
    return <Typography color="textSecondary">Loading...</Typography>;
  }
  const actorGroups =
    debouncedNameFilter === ""
      ? Object.entries(rayletInfo.actorGroups)
      : Object.entries(rayletInfo.actorGroups).filter(([key, _]) =>
          actorClassMatchesSearch(key, debouncedNameFilter),
        );
  return (
    <Box className={classes.container}>
      {actorGroups.length === 0 ? (
        <Typography color="textSecondary">No actors found.</Typography>
      ) : (
        <React.Fragment>
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
          <ActorClassGroups actorGroups={Object.fromEntries(actorGroups)} />
        </React.Fragment>
      )}
    </Box>
  );
};

export default LogicalView;
