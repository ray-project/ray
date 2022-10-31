export const memoryConverter = (bytes: number) => {
  if (bytes < 1024) {
    return `${bytes.toFixed(4)}KB`;
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
    return `${(bytes / 1024 ** 5).toFixed(2)}TB`;
  }

  return "";
};
