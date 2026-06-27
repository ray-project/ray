import { memoryConverter } from "./converter";

describe("memoryConverter", () => {
  const table: { name: string; input: number; expected: string }[] = [
    {
      name: "convert to Bytes",
      input: 4,
      expected: "4.0000B",
    },
    {
      name: "convert to KB",
      input: 5 * 1024 ** 1,
      expected: "5.00KB",
    },
    {
      name: "convert to MB",
      input: 6 * 1024 ** 2,
      expected: "6.00MB",
    },
    {
      name: "convert to GB",
      input: 7 * 1024 ** 3,
      expected: "7.00GB",
    },
    {
      name: "convert to TB",
      input: 8 * 1024 ** 4,
      expected: "8.00TB",
    },
    {
      name: "convert to PB",
      input: 9 * 1024 ** 5,
      expected: "9.00PB",
    },
  ];

  test.each(table)("$name", ({ input, expected }) => {
    expect(memoryConverter(input)).toEqual(expected);
  });

  // The reporter agent serializes psutil process stats with as_dict(), which
  // emits null for fields that psutil cannot read (e.g. pfaults/pageins on
  // some platforms or when AccessDenied is raised). memoryConverter must not
  // crash on null/undefined/NaN, otherwise the Worker table fails to render.
  // See: https://github.com/ray-project/ray/issues (Cannot read properties of
  // null (reading 'toFixed') at converter.ts)
  describe("edge cases", () => {
    test("returns '-' for null instead of throwing", () => {
      expect(() => memoryConverter(null)).not.toThrow();
      expect(memoryConverter(null)).toEqual("-");
    });

    test("returns '-' for undefined instead of throwing", () => {
      expect(() => memoryConverter(undefined)).not.toThrow();
      expect(memoryConverter(undefined)).toEqual("-");
    });

    test("returns '-' for NaN instead of throwing", () => {
      expect(() => memoryConverter(NaN)).not.toThrow();
      expect(memoryConverter(NaN)).toEqual("-");
    });
  });
});
