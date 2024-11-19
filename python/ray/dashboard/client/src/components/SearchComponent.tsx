import { SearchOutlined } from "@mui/icons-material";
import {
  Autocomplete,
  InputAdornment,
  MenuItem,
  TextField,
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
}: {
  serverTimeZone?: TimezoneInfo;
}) => {
  const [timezone, setTimezone] = useState<string>("");

  useEffect(() => {
    if (serverTimeZone) {
      const currentTimezone =
        localStorage.getItem("timezone") ||
        serverTimeZone.value ||
        Intl.DateTimeFormat().resolvedOptions().timeZone;
      formatTimeZone(currentTimezone);
      setTimezone(currentTimezone);
    }
  }, [serverTimeZone]);

  const handleTimezoneChange = (value: string) => {
    localStorage.setItem("timezone", value);
    window.location.reload();
  };

  const options = timezones.sort((a, b) => a.group.localeCompare(b.group));
  options.unshift({
    value: "Etc/UTC",
    utc: "GMT+00:00",
    group: "System",
    country: "Coordinated Universal Time",
  });

  const browserTimezone = Intl.DateTimeFormat().resolvedOptions().timeZone;

  const browserOffset = (() => {
    const match = new Date().toString().match(/([A-Z]+)([+-])(\d{2}):?(\d{2})/);
    if (match) {
      const [, , sign, hours, minutes] = match;
      return `GMT${sign}${hours}:${minutes}`;
    }
    return null;
  })();

  if (browserOffset) {
    options.unshift({
      value: browserTimezone,
      utc: browserOffset,
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
        <li
          {...props}
          style={{
            display: "flex",
            justifyContent: "space-between",
          }}
        >
          <span style={{ marginRight: "10px" }}>{option.country}</span>
          <span style={{ color: "gray" }}>{option.value}</span>
          <span style={{ marginLeft: "auto" }}>{option.utc}</span>
        </li>
      )}
      renderInput={(params) => (
        <TextField {...params} sx={{ width: 120 }} placeholder={curUtc} />
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
