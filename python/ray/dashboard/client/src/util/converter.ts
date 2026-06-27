export const memoryConverter = (bytes: number | undefined | null) => {
  // The reporter agent serializes psutil process stats via as_dict(), which
  // yields null for fields psutil cannot read (e.g. pfaults/pageins on some
  // platforms, or when AccessDenied is raised). Guard against null/undefined
  // /NaN so the Worker table keeps rendering instead of crashing on `.toFixed`.
  if (
    bytes === null ||
    bytes === undefined ||
    typeof bytes !== "number" ||
    isNaN(bytes)
  ) {
    return "-";
  }

  if (bytes < 1024) {
    return `${bytes.toFixed(4)}B`;
  }

  if (bytes < 1024 ** 2) {
    return `${(bytes / 1024 ** 1).toFixed(2)}KB`;
  }

  if (bytes < 1024 ** 3) {
    return `${(bytes / 1024 ** 2).toFixed(2)}MB`;
  }

  if (bytes < 1024 ** 4) {
    return `${(bytes / 1024 ** 3).toFixed(2)}GB`;
  }

  if (bytes < 1024 ** 5) {
    return `${(bytes / 1024 ** 4).toFixed(2)}TB`;
  }

  if (bytes < 1024 ** 6) {
    return `${(bytes / 1024 ** 5).toFixed(2)}PB`;
  }

  return "";
};
