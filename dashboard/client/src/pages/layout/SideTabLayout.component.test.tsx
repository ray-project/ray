import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { MemoryRouter, Route, Routes } from "react-router-dom";

import { SideTabLayout, SideTabPage, SideTabRouteLink } from "./SideTabLayout";

const TestApp = ({ location = "/" }: { location?: string }) => {
  return (
    <MemoryRouter initialEntries={[location]}>
      <Routes>
        <Route
          element={
            <SideTabLayout>
              <SideTabRouteLink tabId="tab-a" title="Tab A" to="tab-a" />
              <SideTabRouteLink tabId="tab-b" title="Tab B" to="tab-b" />
            </SideTabLayout>
          }
        >
          <Route
            element={
              <SideTabPage tabId="tab-a">
                <div>Page Contents A</div>
              </SideTabPage>
            }
            path="tab-a"
          />
          <Route
            element={
              <SideTabPage tabId="tab-b">
                <div>Page Contents B</div>
              </SideTabPage>
            }
            path="tab-b"
          />
          <Route element={<div>Not a tabbed page</div>} path="page-c" />
        </Route>
      </Routes>
    </MemoryRouter>
  );
};

describe("SideTabLayout", () => {
  it("navigates and renders correctly", async () => {
    const user = userEvent.setup();

    render(<TestApp location="/tab-a" />);
    await screen.findByText(/Page Contents A/);
    expect(screen.queryByText(/Page Contents B/)).toBeNull();
    expect(screen.getByRole("tab", { selected: true })).toHaveTextContent(
      "tab-a",
    );

    // Go to tab b
    await user.click(screen.getByText(/tab-b/));
    await screen.findByText(/Page Contents B/);
    expect(screen.queryByText(/Page Contents A/)).toBeNull();
    expect(screen.getByRole("tab", { selected: true })).toHaveTextContent(
      "tab-b",
    );

    cleanup();

    // Go to non tab page and make sure nothing is broken
    render(<TestApp location="/page-c" />);
    await screen.findByText(/Not a tabbed page/);
    expect(screen.queryByText(/Page Contents A/)).toBeNull();
    expect(screen.queryByText(/Page Contents B/)).toBeNull();
    expect(screen.queryByRole("tab", { selected: true })).toBeNull();

    // Go to tab a
    await user.click(screen.getByText(/tab-a/));
    await screen.findByText(/Page Contents A/);
    expect(screen.queryByText(/Not a tabbed page/)).toBeNull();
    expect(screen.queryByText(/Page Contents B/)).toBeNull();
    expect(screen.getByRole("tab", { selected: true })).toHaveTextContent(
      "tab-a",
    );
  });
});
