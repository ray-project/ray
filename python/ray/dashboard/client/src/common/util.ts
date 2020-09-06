import { useEffect, useRef } from "react";

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

export const filterObj = (obj: Object, filterFn: any) =>
  Object.fromEntries(Object.entries(obj).filter(filterFn));

export const mapObj = (obj: Object, filterFn: any) =>
  Object.fromEntries(Object.entries(obj).map(filterFn));

export const useInterval = (callback: Function, delayMs: number) => {
  const savedCallback = useRef<any>();
  const intervalId = useRef<any>();
  useEffect(() => {
    savedCallback.current = callback;
  }, [callback]);
  useEffect(() => {
    const tick = () => savedCallback?.current();
    intervalId.current = setInterval(tick, delayMs);
    savedCallback.current();
    return () => {
      if (intervalId.current) {
        clearInterval(intervalId.current);
      }
    };
  }, [callback, delayMs]);
  return intervalId.current
    ? () => clearInterval(intervalId.current)
    : () => null;
};
