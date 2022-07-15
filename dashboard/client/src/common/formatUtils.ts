export const formatByteAmount = (
  amount: number,
  unit: "mebibyte" | "gibibyte",
) =>
  `${(
    amount / (unit === "mebibyte" ? Math.pow(1024, 2) : Math.pow(1024, 3))
  ).toFixed(1)} ${unit === "mebibyte" ? "MiB" : "GiB"}`;

export const formatUsage = (
  used_b: number,
  total_b: number,
  unit: "mebibyte" | "gibibyte",
  includePercentage: boolean,
) => {
  const usedFormatted = formatByteAmount(used_b, unit);
  const totalFormatted = formatByteAmount(total_b, unit);
  const percent = (100 * used_b) / total_b;
  const ratioStr = `${usedFormatted} / ${totalFormatted}`;
  if (includePercentage) {
    return `${ratioStr} (${percent.toFixed(0)}%)`;
  }
  return ratioStr;
};

// Formats, e.g. 400 and 6000 as "400 MiB / 6000 MiB (6.7%)"
export const MiBRatio = (used: number, total: number) =>
  `${used} MiB / ${total} MiB (${(100 * (used / total)).toFixed(1)}%)`;

export const MiBRatioNoPercent = (used: number, total: number) =>
  `${used} MiB / ${total} MiB`;

export const formatDuration = (durationInSeconds: number) => {
  const durationSeconds = Math.floor(durationInSeconds) % 60;
  const durationMinutes = Math.floor(durationInSeconds / 60) % 60;
  const durationHours = Math.floor(durationInSeconds / 60 / 60) % 24;
  const durationDays = Math.floor(durationInSeconds / 60 / 60 / 24);
  const pad = (value: number) => value.toString().padStart(2, "0");
  return [
    durationDays ? `${durationDays}d` : "",
    `${pad(durationHours)}h`,
    `${pad(durationMinutes)}m`,
    `${pad(durationSeconds)}s`,
  ].join(" ");
};

export const formatValue = (rawFloat: number) => {
  try {
    const decimals = rawFloat.toString().split(".")[1].length || 0;
    if (decimals <= 3) {
      return rawFloat.toString();
    } // Few decimals
    if (Math.abs(rawFloat.valueOf()) >= 1.0) {
      return rawFloat.toPrecision(5);
    } // Values >= 1
    return rawFloat.toExponential(); // Values in (-1; 1)
  } catch (e) {
    return rawFloat.toString();
  }
};
