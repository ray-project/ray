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
  serverTimeZoneLoaded,
}: {
  serverTimeZone?: string | null;
  serverTimeZoneLoaded?: boolean;
}) => {
  const [timezone, setTimezone] = useState<string>("");

  useEffect(() => {
    if (serverTimeZoneLoaded) {
      const currentTimezone =
        localStorage.getItem("timezone") ||
        serverTimeZone ||
        Intl.DateTimeFormat().resolvedOptions().timeZone;
      formatTimeZone(currentTimezone);
      setTimezone(currentTimezone);
    }
  }, [serverTimeZoneLoaded, serverTimeZone]);

  const handleTimezoneChange = (value: string) => {
    localStorage.setItem("timezone", value);
    window.location.reload();
  };

  const options = timezones.sort((a, b) => a.group.localeCompare(b.group));
  const browserTimezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
  const browerUtc =
    timezones.find((t) => t.value === browserTimezone)?.utc || "";
  const specificValue = [
    {
      value: browserTimezone,
      utc: browerUtc,
      group: "System",
      country: "Browser Time",
    },
    {
      value: "Etc/UTC",
      utc: "GMT+00:00",
      group: "System",
      country: "Coordinated Universal Time",
    },
  ];
  specificValue.forEach((val) => {
    options.unshift(val);
  });

  if (serverTimeZone) {
    const serverUtc =
      timezones.find((t) => t.value === serverTimeZone)?.utc || "";
    options.unshift({
      value: serverTimeZone,
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
