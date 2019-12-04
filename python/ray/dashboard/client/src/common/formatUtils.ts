export const formatByteAmount = (
  amount: number,
  unit: "mebibyte" | "gibibyte"
) =>
  `${(
    amount / (unit === "mebibyte" ? Math.pow(1024, 2) : Math.pow(1024, 3))
  ).toFixed(1)} ${unit === "mebibyte" ? "MiB" : "GiB"}`;

export const formatUsage = (
  used: number,
  total: number,
  unit: "mebibyte" | "gibibyte"
) => {
  const usedFormatted = formatByteAmount(used, unit);
  const totalFormatted = formatByteAmount(total, unit);
  const percent = (100 * used) / total;
  return `${usedFormatted} / ${totalFormatted} (${percent.toFixed(0)}%)`;
};

export const formatUptime = (bootTime: number) => {
  const uptimeSecondsTotal = Date.now() / 1000 - bootTime;
  const uptimeSeconds = Math.floor(uptimeSecondsTotal) % 60;
  const uptimeMinutes = Math.floor(uptimeSecondsTotal / 60) % 60;
  const uptimeHours = Math.floor(uptimeSecondsTotal / 60 / 60) % 24;
  const uptimeDays = Math.floor(uptimeSecondsTotal / 60 / 60 / 24);
  const pad = (value: number) => value.toString().padStart(2, "0");
  return [
    uptimeDays ? `${uptimeDays}d` : "",
    `${pad(uptimeHours)}h`,
    `${pad(uptimeMinutes)}m`,
    `${pad(uptimeSeconds)}s`
  ].join(" ");
};
