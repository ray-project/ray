import {
  InputAdornment,
  makeStyles,
  MenuItem,
  TextField,
} from "@material-ui/core";
import { SearchOutlined } from "@material-ui/icons";
import React from "react";

const useStyles = makeStyles((theme) => ({
  search: {
    margin: theme.spacing(1),
    marginTop: 0,
  },
}));

export const SearchInput = ({
  label,
  onChange,
}: {
  label: string;
  onChange?: (value: string) => void;
}) => {
  const classes = useStyles();

  return (
    <TextField
      className={classes.search}
      size="small"
      label={label}
      InputProps={{
        onChange: ({ target: { value } }) => {
          if (onChange) {
            onChange(value);
          }
        },
        endAdornment: (
          <InputAdornment position="end">
            <SearchOutlined />
          </InputAdornment>
        ),
      }}
    />
  );
};

export const SearchSelect = ({
  label,
  onChange,
  options,
}: {
  label: string;
  onChange?: (value: string) => void;
  options: string[];
}) => {
  const classes = useStyles();
  return (
    <TextField
      className={classes.search}
      size="small"
      label={label}
      select
      SelectProps={{
        onChange: ({ target: { value } }) => {
          if (onChange) {
            onChange(value as string);
          }
        },
        style: {
          width: 100,
        },
      }}
    >
      <MenuItem value="">All</MenuItem>
      {options.map((e) => (
        <MenuItem value={e}>{e}</MenuItem>
      ))}
    </TextField>
  );
};
