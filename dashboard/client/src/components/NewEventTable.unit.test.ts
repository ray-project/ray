import { appendToParams, transformFiltersToParams } from "./NewEventTableUtils";

describe("appendToParams", () => {
  it.each([
    ["key", "value", "key=value"],
    ["key", ["value1", "value2"], "key=value1&key=value2"],
  ])(
    "should append values correctly for input key: %s and value: %j",
    (key, value, expected) => {
      const params = new URLSearchParams();
      appendToParams(params, key, value as string | string[]);
      expect(params.toString()).toBe(expected);
    },
  );
});

describe("transformFiltersToParams", () => {
  it("should return empty string for null filters", () => {
    expect(transformFiltersToParams(null)).toBe("");
  });

  it.each([
    [{ entityName: "job_id", entityId: "12345" }, "job_id=12345"],
    [
      { count: "10", sourceType: ["GCS"], severityLevel: ["INFO", "ERROR"] },
      expect.stringContaining(
        "count=10&sourceType=GCS&severityLevel=INFO&severityLevel=ERROR",
      ),
    ],
    [
      { entityName: "serve_app_name", entityId: "12345", count: "10" },
      expect.stringContaining("serve_app_name=12345&count=10"),
    ],
  ])("should transform filters: %j", (filters: any, expected) => {
    const result = transformFiltersToParams(filters);
    expect(result).toEqual(expected);
  });
});
