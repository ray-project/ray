import { Filters } from "../type/event";

export const appendToParams = (
  params: URLSearchParams,
  key: string,
  value: string | string[],
) => {
  if (Array.isArray(value)) {
    value.forEach((val) =>
      params.append(encodeURIComponent(key), encodeURIComponent(val)),
    );
  } else {
    params.append(encodeURIComponent(key), encodeURIComponent(value));
  }
};

export const transformFiltersToParams = (filters: Filters | null) => {
  const params = new URLSearchParams();

  if (!filters) {
    return "";
  }

  // Handling special cases for entityName and entityId
  if (filters.entityName && filters.entityId) {
    appendToParams(params, filters.entityName, filters.entityId);
  }

  // Handling general cases, for key like "count", "sourceType", "severityLevel"
  Object.entries(filters).forEach(([key, value]) => {
    if (key !== "entityName" && key !== "entityId") {
      appendToParams(params, key, value as string | string[]);
    }
  });

  return params.toString();
};
