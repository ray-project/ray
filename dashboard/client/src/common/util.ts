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
