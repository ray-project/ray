import { SearchOutlined } from "@mui/icons-material";
import {
  Autocomplete,
  Box,
  Divider,
  InputAdornment,
  MenuItem,
  TextField,
  Typography,
} from "@mui/material";

import React, { useEffect, useState } from "react";
import { formatTimeZone } from "../common/formatUtils";
import { timezones } from "../common/timezone";
import { TimezoneInfo } from "../pages/metrics/utils";

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
      defaultValue={defaultValue || ""}
    >
      {showAllOption ? <MenuItem value="">All</MenuItem> : null}
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

export const SearchTimezone = ({
  serverTimeZone,
  currentTimeZone,
}: {
  serverTimeZone?: TimezoneInfo | null;
  currentTimeZone?: string;
}) => {
  const [timezone, setTimezone] = useState<string>("");

  useEffect(() => {
    if (currentTimeZone !== undefined) {
      formatTimeZone(currentTimeZone);
      setTimezone(currentTimeZone);
    }
  }, [currentTimeZone]);

  const handleTimezoneChange = (value: string) => {
    localStorage.setItem("timezone", value);
    window.location.reload();
  };

  const options = timezones
    .map((x) => x) // Create a copy
    .sort((a, b) => a.group.localeCompare(b.group));
  options.unshift({
    value: "Etc/UTC",
    utc: "GMT+00:00",
    group: "System",
    country: "Coordinated Universal Time",
  });

  const browserTimezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
  const browserUtc = timezones.find((t) => t.value === browserTimezone)?.utc;
  if (browserUtc) {
    options.unshift({
      value: browserTimezone,
      utc: browserUtc,
      group: "System",
      country: "Browser Time",
    });
  }

  const serverUtc =
    serverTimeZone?.value &&
    timezones.find((t) => t.value === serverTimeZone.value)?.utc;
  if (serverUtc) {
    options.unshift({
      value: serverTimeZone.value,
      utc: serverUtc,
      group: "System",
      country: "Dashboard Server Timezone",
    });
  }

  const curUtc = timezones.find((t) => t.value === timezone)?.utc;
  return (
    <Autocomplete
      size="small"
      onChange={(event, newValue) => {
        if (newValue) {
          handleTimezoneChange(newValue.value);
        }
      }}
      options={options}
      getOptionLabel={(option) => option.value}
      groupBy={(option) => option.group}
      filterOptions={(options, { inputValue }) =>
        options.filter(
          (item) =>
            item.value.includes(inputValue) ||
            item.utc.includes(inputValue) ||
            item.country.toLowerCase().includes(inputValue.toLowerCase()) ||
            item.group.toLowerCase().includes(inputValue.toLowerCase()),
        )
      }
      renderOption={(props, option) => (
        <Box
          component="li"
          {...props}
          sx={{
            display: "flex",
            justifyContent: "space-between",
          }}
        >
          <Typography component="span" sx={{ marginRight: 1 }}>
            {option.country}
          </Typography>
          <Typography sx={{ color: "#8C9196" }} component="span">
            {option.value}
          </Typography>
          <Box sx={{ flexGrow: 1 }} />
          <Typography component="span" sx={{ marginLeft: 1 }}>
            {option.utc}
          </Typography>
        </Box>
      )}
      renderInput={(params) => (
        <TextField
          {...params}
          sx={{
            width: 120,
            "& .MuiOutlinedInput-notchedOutline": {
              borderColor: "#D2DCE6",
            },
          }}
          placeholder={curUtc}
        />
      )}
      renderGroup={(params) => (
        <li>
          <Typography sx={{ color: "#5F6469", paddingX: 2, paddingY: "6px" }}>
            {params.group}
          </Typography>
          <Box
            component="ul"
            sx={{
              padding: 0,
            }}
          >
            {params.children}
          </Box>
          <Divider
            sx={{
              marginX: 2,
              marginY: 1,
            }}
          />
        </li>
      )}
      slotProps={{
        paper: {
          style: {
            width: "400px",
          },
        },
        popper: {
          placement: "bottom-end",
          style: {
            width: "fit-content",
          },
        },
      }}
    />
  );
};
