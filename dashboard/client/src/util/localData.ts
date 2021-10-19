export const getLocalStorage = <T>(key: string) => {
  const data = window.localStorage.getItem(key);
  try {
    return JSON.parse(data || "") as T;
  } catch {
    return data;
  }
};

export const setLocalStorage = (key: string, value: any) => {
  return window.localStorage.setItem(key, JSON.stringify(value));
};
