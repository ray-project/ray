/**
 * Cookie utility functions for Ray dashboard authentication.
 */

const AUTHENTICATION_TOKEN_COOKIE_NAME = "ray-authentication-token";

/**
 * Get a cookie value by name.
 *
 * @param name - The name of the cookie to retrieve
 * @returns The cookie value if found, null otherwise
 */
export const getCookie = (name: string): string | null => {
  const nameEQ = name + "=";
  const cookies = document.cookie.split(";");

  for (let i = 0; i < cookies.length; i++) {
    let cookie = cookies[i];
    while (cookie.charAt(0) === " ") {
      cookie = cookie.substring(1, cookie.length);
    }
    if (cookie.indexOf(nameEQ) === 0) {
      return cookie.substring(nameEQ.length, cookie.length);
    }
  }
  return null;
};

/**
 * Set a cookie with the given name, value, and expiration.
 *
 * @param name - The name of the cookie
 * @param value - The value to store in the cookie
 * @param days - Number of days until the cookie expires (default: 30)
 */
export const setCookie = (name: string, value: string, days = 30): void => {
  let expires = "";
  if (days) {
    const date = new Date();
    date.setTime(date.getTime() + days * 24 * 60 * 60 * 1000);
    expires = "; expires=" + date.toUTCString();
  }
  document.cookie = name + "=" + (value || "") + expires + "; path=/";
};

/**
 * Delete a cookie by name.
 *
 * @param name - The name of the cookie to delete
 */
export const deleteCookie = (name: string): void => {
  document.cookie = name + "=; Max-Age=-99999999; path=/";
};

/**
 * Get the authentication token from cookies.
 *
 * @returns The authentication token if found, null otherwise
 */
export const getAuthenticationToken = (): string | null => {
  return getCookie(AUTHENTICATION_TOKEN_COOKIE_NAME);
};

/**
 * Set the authentication token in cookies.
 *
 * @param token - The authentication token to store
 */
export const setAuthenticationToken = (token: string): void => {
  setCookie(AUTHENTICATION_TOKEN_COOKIE_NAME, token);
};

/**
 * Clear the authentication token from cookies.
 */
export const clearAuthenticationToken = (): void => {
  deleteCookie(AUTHENTICATION_TOKEN_COOKIE_NAME);
};
