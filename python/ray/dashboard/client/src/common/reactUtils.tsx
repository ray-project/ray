import { useRef, useEffect } from 'react';
const noop = () => { };

export const useInterval = (callback: () => void, delayMs: number) => {
  const savedCallback = useRef(noop);
  const intervalId = useRef<any>(null);
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
    }
  }, [delayMs]);
  return intervalId.current !== null ?
    () => clearInterval(intervalId.current) :
    () => null;
}
