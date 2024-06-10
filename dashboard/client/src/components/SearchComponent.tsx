import { SearchOutlined } from "@mui/icons-material";
import { InputAdornment, MenuItem, TextField } from "@mui/material";
import React from "react";

export const SearchInput = ({
  label,
  onChange,
  defaultValue,
}: {
  label: string;
  defaultValue?: string;
  onChange?: (value: string) => void;
}) => {
  return (
    <TextField
      size="small"
      label={label}
      InputProps={{
        onChange: ({ target: { value } }) => {
          if (onChange) {
            onChange(value);
          }
        },
        defaultValue,
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
  showAllOption,
  defaultValue,
}: {
  label: string;
  onChange?: (value: string) => void;
  options: (string | [string, string])[];
  showAllOption: boolean;
  defaultValue?: string;
}) => {
  return (
    <TextField
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
      defaultValue={defaultValue}
    >
      {showAllOption ?? <MenuItem value="">All</MenuItem>}
      {options.map((e) =>
        typeof e === "string" ? (
          <MenuItem key={e} value={e}>
            {e}
          </MenuItem>
        ) : (
          <MenuItem key={e[0]} value={e[0]}>
            {e[1]}
          </MenuItem>
        ),
      )}
    </TextField>
  );
};
