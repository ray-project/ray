export const formatByteAmount = (
  amount: number,
  unit: "mebibyte" | "gibibyte",
) =>
  `${(
    amount / (unit === "mebibyte" ? Math.pow(1024, 2) : Math.pow(1024, 3))
  ).toFixed(1)} ${unit === "mebibyte" ? "MiB" : "GiB"}`;

export const formatUsage = (
  used: number,
  total: number,
  unit: "mebibyte" | "gibibyte",
) => {
  const usedFormatted = formatByteAmount(used, unit);
  const totalFormatted = formatByteAmount(total, unit);
  const percent = (100 * used) / total;
  return `${usedFormatted} / ${totalFormatted} (${percent.toFixed(0)}%)`;
};

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
