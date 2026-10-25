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
});
