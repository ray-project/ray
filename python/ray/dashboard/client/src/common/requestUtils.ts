const base =
  process.env.NODE_ENV === "development"
    ? "http://localhost:8265"
    : window.location.origin;

// TODO(mitchellstern): Add JSON schema validation for the responses.
export const get = async <T>(path: string, params: { [key: string]: any }) => {
  const url = new URL(path, base);
  for (const [key, value] of Object.entries(params)) {
    url.searchParams.set(key, value);
  }

  const response = await fetch(url.toString());
  const json = await response.json();

  const { result, error } = json;

  if (error) {
    throw Error(error);
  }

  return result as T;
};

export const getv2 = async <T>(
  path: string,
  params: { [key: string]: any },
) => {
  const url = new URL(path, base);
  for (const [key, value] of Object.entries(params)) {
    url.searchParams.set(key, value);
  }

  const response = await fetch(url.toString());
  const json = await response.json();

  const { result, msg, data } = json;

  if (!result) {
    throw Error(msg);
  }

  return data as T;
};

export const post = async <T>(path: string, params: { [key: string]: any }) => {
  const requestOptions = {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(params),
  };

  const url = new URL(path, base);

  const response = await fetch(url.toString(), requestOptions);
  const json = await response.json();

  const { result, error } = json;

  if (error !== null) {
    throw Error(error);
  }

  return result as T;
};
