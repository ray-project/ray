import "@testing-library/jest-dom";
import dayjs from "dayjs";
import duration from "dayjs/plugin/duration";

dayjs.extend(duration);

Object.defineProperty(document, "visibilityState", {
  value: "visible",
  writable: true,
  configurable: true,
});
