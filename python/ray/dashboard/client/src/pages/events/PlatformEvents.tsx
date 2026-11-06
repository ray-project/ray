import { SearchOutlined } from "@mui/icons-material";
import {
  Autocomplete,
  Box,
  InputAdornment,
  Pagination,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  TextFieldProps,
  Typography,
} from "@mui/material";
import React, { useEffect, useMemo, useState } from "react";
import { formatDateFromTimeMs } from "../../common/formatUtils";
import { sliceToPage } from "../../common/util";
import { StatusChip } from "../../components/StatusChip";
import { PlatformEvent } from "../../type/platform";
import { useFilter } from "../../util/hook";

const SeverityChip = ({ severity }: { severity: string }) => {
  return <StatusChip type="severity" status={severity} />;
};

type FilterKey =
  | "severity"
  | "platformEvent.source.platform"
  | "platformEvent.objectKind";

const DEFAULT_PAGE_SIZE = 25;

export const PlatformEvents = ({
  events,
  emptyMessage = "No platform events found.",
  isLoading = false,
}: {
  events: PlatformEvent[];
  emptyMessage?: string;
  isLoading?: boolean;
}) => {
  const [pageNo, setPageNo] = useState(1);
  const [pageSize, setPageSize] = useState(DEFAULT_PAGE_SIZE);
  const [searchText, setSearchText] = useState("");
  const { changeFilter: _changeFilter, filterFunc } = useFilter<FilterKey>();
  const changeFilter = (key: FilterKey, val: string) => {
    _changeFilter(key, val);
    setPageNo(1);
  };
  const severityOptions = useMemo(
    () => Array.from(new Set(events.map((e) => e.severity).filter(Boolean))),
    [events],
  );
  const platformOptions = useMemo(
    () =>
      Array.from(
        new Set(
          events
            .map((e) => e.platformEvent?.source?.platform)
            .filter((v): v is string => !!v),
        ),
      ),
    [events],
  );
  const objectKindOptions = useMemo(
    () =>
      Array.from(
        new Set(
          events
            .map((e) => e.platformEvent?.objectKind)
            .filter((v): v is string => !!v),
        ),
      ),
    [events],
  );
  const filteredEvents: PlatformEvent[] = useMemo(() => {
    const needle = searchText.trim().toLowerCase();
    return events.filter((e: PlatformEvent) => {
      if (!filterFunc(e)) {
        return false;
      }
      if (!needle) {
        return true;
      }
      const msg = (e.platformEvent?.message || e.message || "").toLowerCase();
      const reason = (e.platformEvent?.reason || "").toLowerCase();
      return msg.includes(needle) || reason.includes(needle);
    });
  }, [events, filterFunc, searchText]);

  const {
    items: pageEvents,
    constrainedPage,
    maxPage,
  } = sliceToPage(filteredEvents, pageNo, pageSize);

  const effectivePage = Math.max(1, constrainedPage);
  const pageCount = Math.max(1, maxPage);

  useEffect(() => {
    if (effectivePage !== pageNo) {
      setPageNo(effectivePage);
    }
  }, [effectivePage, pageNo]);

  if (isLoading) {
    return (
      <Box sx={{ padding: 2, textAlign: "center" }}>
        <Typography color="textSecondary">Loading events...</Typography>
      </Box>
    );
  }

  if (!events || events.length === 0) {
    return (
      <Box sx={{ padding: 2, textAlign: "center" }}>
        <Typography color="textSecondary">{emptyMessage}</Typography>
      </Box>
    );
  }

  return (
    <Box>
      <Box
        sx={{
          display: "flex",
          flexWrap: "wrap",
          alignItems: "center",
          gap: 1,
          marginBottom: 1,
        }}
      >
        <Autocomplete
          sx={{ width: 160 }}
          size="small"
          options={severityOptions}
          onInputChange={(_: any, value: string) =>
            changeFilter("severity", value.trim())
          }
          renderInput={(params: TextFieldProps) => (
            <TextField {...params} label="Severity" />
          )}
        />
        <Autocomplete
          sx={{ width: 200 }}
          size="small"
          options={platformOptions}
          onInputChange={(_: any, value: string) =>
            changeFilter("platformEvent.source.platform", value.trim())
          }
          renderInput={(params: TextFieldProps) => (
            <TextField {...params} label="Platform" />
          )}
        />
        <Autocomplete
          sx={{ width: 200 }}
          size="small"
          options={objectKindOptions}
          onInputChange={(_: any, value: string) =>
            changeFilter("platformEvent.objectKind", value.trim())
          }
          renderInput={(params: TextFieldProps) => (
            <TextField {...params} label="Object Kind" />
          )}
        />
        <TextField
          sx={{ width: 240 }}
          size="small"
          label="Search reason / message"
          value={searchText}
          onChange={(e) => {
            setSearchText(e.target.value);
            setPageNo(1);
          }}
          InputProps={{
            endAdornment: (
              <InputAdornment position="end">
                <SearchOutlined
                  sx={(theme) => ({ color: theme.palette.text.secondary })}
                />
              </InputAdornment>
            ),
          }}
        />
        <TextField
          sx={{ width: 120 }}
          size="small"
          label="Page size"
          type="number"
          value={pageSize}
          onChange={(e) => {
            const n = Math.min(
              500,
              Math.max(1, Number(e.target.value) || DEFAULT_PAGE_SIZE),
            );
            setPageSize(n);
            setPageNo(1);
          }}
        />
      </Box>
      <Box
        sx={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          marginBottom: 1,
        }}
      >
        <Typography variant="body2" color="textSecondary">
          {filteredEvents.length === events.length
            ? `${events.length} events`
            : `${filteredEvents.length} of ${events.length} events`}
        </Typography>
        <Pagination
          count={pageCount}
          page={effectivePage}
          onChange={(_: React.ChangeEvent<unknown>, value: number) =>
            setPageNo(value)
          }
          size="small"
        />
      </Box>

      {pageEvents.length === 0 ? (
        <Box sx={{ padding: 2, textAlign: "center" }}>
          <Typography color="textSecondary">
            No events match the current filters.
          </Typography>
        </Box>
      ) : (
        <TableContainer>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>Time</TableCell>
                <TableCell>Severity</TableCell>
                <TableCell>Source</TableCell>
                <TableCell>Object</TableCell>
                <TableCell>Reason</TableCell>
                <TableCell>Message</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {pageEvents.map((event: PlatformEvent, index: number) => {
                const pEvent = event.platformEvent;
                const sourceStr = pEvent?.source
                  ? `${pEvent.source.platform}/${pEvent.source.component}`
                  : "-";
                const objectStr = pEvent?.objectKind
                  ? `${pEvent.objectKind}/${pEvent.objectName}`
                  : "-";
                const timeMs = new Date(event.timestamp).getTime();

                return (
                  <TableRow key={event.eventId || index}>
                    <TableCell>
                      {isNaN(timeMs)
                        ? event.timestamp
                        : formatDateFromTimeMs(timeMs)}
                    </TableCell>
                    <TableCell>
                      <SeverityChip severity={event.severity} />
                    </TableCell>
                    <TableCell>{sourceStr}</TableCell>
                    <TableCell>{objectStr}</TableCell>
                    <TableCell>{pEvent?.reason || "-"}</TableCell>
                    <TableCell
                      sx={{ whiteSpace: "pre-wrap", wordBreak: "break-word" }}
                    >
                      {pEvent?.message || event.message || "-"}
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </TableContainer>
      )}

      {maxPage > 1 && (
        <Box sx={{ display: "flex", justifyContent: "flex-end", marginTop: 1 }}>
          <Pagination
            count={pageCount}
            page={effectivePage}
            onChange={(_: React.ChangeEvent<unknown>, value: number) =>
              setPageNo(value)
            }
            size="small"
          />
        </Box>
      )}
    </Box>
  );
};
