import _ from "lodash";

export const getWeightedAverage = (
  input: {
    weight: number;
    value: number;
  }[],
) => {
  if (input.length === 0) {
    return 0;
  }

  let totalWeightTimesValue = 0;
  let totalWeight = 0;
  for (const { weight, value } of input) {
    totalWeightTimesValue += weight * value;
    totalWeight += weight;
  }
  return totalWeightTimesValue / totalWeight;
};

export const sum = (vals: number[]) => vals.reduce((acc, val) => acc + val, 0);

export const filterObj = (obj: Record<string, unknown>, filterFn: any) =>
  Object.fromEntries(Object.entries(obj).filter(filterFn));

export const mapObj = (obj: Record<string, unknown>, filterFn: any) =>
  Object.fromEntries(Object.entries(obj).map(filterFn) as any[]);

export const filterRuntimeEnvSystemVariables = (
  runtime_env: Record<string, any>,
): Record<string, any> => {
  const out = _.pickBy(runtime_env, (_, key) => {
    if (key.startsWith("_")) {
      return false;
    }
    return true;
  });
  return out;
};

export const sliceToPage = <T>(
  items: T[],
  pageNo: number,
  pageSize = 10,
): { items: T[]; constrainedPage: number; maxPage: number } => {
  const maxPage = Math.ceil(items.length / pageSize);
  const constrainedPage = Math.min(maxPage, Math.max(1, pageNo));
  const start = (constrainedPage - 1) * pageSize;
  const end = constrainedPage * pageSize;
  return {
    items: items.slice(start, end),
    constrainedPage,
    maxPage,
  };
};
