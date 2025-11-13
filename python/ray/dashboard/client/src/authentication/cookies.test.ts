import "@testing-library/jest-dom";
import {
  clearAuthenticationToken,
  deleteCookie,
  getAuthenticationToken,
  getCookie,
  setAuthenticationToken,
  setCookie,
} from "./cookies";

describe("Cookie utilities", () => {
  beforeEach(() => {
    // Clear all cookies before each test
    document.cookie.split(";").forEach((cookie) => {
      const name = cookie.split("=")[0].trim();
      document.cookie = `${name}=; expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/;`;
    });
  });

  describe("setCookie and getCookie", () => {
    it("sets and retrieves a cookie", () => {
      setCookie("test-cookie", "test-value");
      const value = getCookie("test-cookie");
      expect(value).toBe("test-value");
    });

    it("returns null for non-existent cookie", () => {
      const value = getCookie("non-existent");
      expect(value).toBeNull();
    });

    it("overwrites existing cookie with same name", () => {
      setCookie("test-cookie", "value1");
      setCookie("test-cookie", "value2");
      const value = getCookie("test-cookie");
      expect(value).toBe("value2");
    });
  });

  describe("deleteCookie", () => {
    it("deletes an existing cookie", () => {
      setCookie("test-cookie", "test-value");
      expect(getCookie("test-cookie")).toBe("test-value");

      deleteCookie("test-cookie");
      expect(getCookie("test-cookie")).toBeNull();
    });

    it("handles deletion of non-existent cookie", () => {
      // Should not throw error
      expect(() => deleteCookie("non-existent")).not.toThrow();
    });
  });

  describe("Authentication token functions", () => {
    it("sets and retrieves authentication token", () => {
      const testToken = "test-auth-token-123";
      setAuthenticationToken(testToken);

      const retrievedToken = getAuthenticationToken();
      expect(retrievedToken).toBe(testToken);
    });

    it("returns null when no authentication token is set", () => {
      const token = getAuthenticationToken();
      expect(token).toBeNull();
    });

    it("clears authentication token", () => {
      setAuthenticationToken("test-token");
      expect(getAuthenticationToken()).toBe("test-token");

      clearAuthenticationToken();
      expect(getAuthenticationToken()).toBeNull();
    });

    it("overwrites existing authentication token", () => {
      setAuthenticationToken("token1");
      expect(getAuthenticationToken()).toBe("token1");

      setAuthenticationToken("token2");
      expect(getAuthenticationToken()).toBe("token2");
    });
  });

  describe("Multiple cookies", () => {
    it("handles multiple cookies independently", () => {
      setCookie("cookie1", "value1");
      setCookie("cookie2", "value2");
      setCookie("cookie3", "value3");

      expect(getCookie("cookie1")).toBe("value1");
      expect(getCookie("cookie2")).toBe("value2");
      expect(getCookie("cookie3")).toBe("value3");
    });

    it("deletes only specified cookie", () => {
      setCookie("cookie1", "value1");
      setCookie("cookie2", "value2");

      deleteCookie("cookie1");

      expect(getCookie("cookie1")).toBeNull();
      expect(getCookie("cookie2")).toBe("value2");
    });
  });
});
