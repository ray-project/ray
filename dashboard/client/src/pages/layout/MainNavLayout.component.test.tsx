import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { Link, MemoryRouter, Outlet, Route, Routes } from "react-router-dom";
import { MainNavPageInfo } from "./mainNavContext";
import { MainNavLayout } from "./MainNavLayout";

const TestPageA = () => {
  return (
    <div>
      <MainNavPageInfo
        pageInfo={{ title: "breadcrumb-a1", id: "a1", path: "/a1" }}
      />
      Page A1
      <Link to="a2">go-a2</Link>
      <Outlet />
    </div>
  );
};
const TestChildPageA = () => {
  return (
    <div>
      <MainNavPageInfo
        pageInfo={{ title: "breadcrumb-a2", id: "a2", path: "/a1/a2" }}
      />
      Page A2
      <Link to="a3">go-a3</Link>
      <Outlet />
    </div>
  );
};
const TestGrandChildPageA = () => {
  return (
    <div>
      <MainNavPageInfo
        pageInfo={{ title: "breadcrumb-a3", id: "a3", path: "/a1/a2/a3" }}
      />
      Page A3
      <Link to="/b1/b2">go-b2</Link>
    </div>
  );
};
const TestPageB = () => {
  return (
    <div>
      <MainNavPageInfo
        pageInfo={{ title: "breadcrumb-b1", id: "b1", path: "/b1" }}
      />
      Page B1
      <Outlet />
    </div>
  );
};
const TestChildNonMainNavPageB = () => {
  return (
    <div>
      {/* Not a Main Nav page */}
      Page B2
      <Link to="b3">go-b3</Link>
      <Outlet />
    </div>
  );
};
const TestGrandChildPageB = () => {
  return (
    <div>
      <MainNavPageInfo
        pageInfo={{ title: "breadcrumb-b3", id: "b3", path: "/b1/b2/b3" }}
      />
      Page B3
      <Link to="/c">go-c</Link>
    </div>
  );
};
const TestPageC = () => {
  return (
    <div>
      <MainNavPageInfo
        pageInfo={{ title: "breadcrumbc", id: "c", path: "/c" }}
      />
      Page C
    </div>
  );
};

const TestApp = ({ location = "/" }: { location?: string }) => {
  return (
    <MemoryRouter initialEntries={[location]}>
      <Routes>
        <Route element={<MainNavLayout />}>
          <Route element={<TestPageA />} path="a1">
            <Route element={<TestChildPageA />} path="a2">
              <Route element={<TestGrandChildPageA />} path="a3" />
            </Route>
          </Route>
          <Route element={<TestPageB />} path="b1">
            <Route element={<TestChildNonMainNavPageB />} path="b2">
              <Route element={<TestGrandChildPageB />} path="b3" />
            </Route>
          </Route>
          <Route element={<TestPageC />} path="c" />
        </Route>
      </Routes>
    </MemoryRouter>
  );
};

describe("MainNavLayout", () => {
  it("navigates and renders breadcrumbs correctly", async () => {
    const user = userEvent.setup();

    render(<TestApp location="/a1" />);
    await screen.findByText(/Page A1/);
    // No breadcrumbs when there is only 1 page
    expect(screen.queryByText(/breadcrumb-a1/)).not.toBeInTheDocument();

    await user.click(screen.getByText(/go-a2/));
    await screen.findByText(/Page A2/);
    expect(screen.getByText(/breadcrumb-a1/)).toBeInTheDocument();
    expect(screen.getByText(/breadcrumb-a2/)).toBeInTheDocument();

    await user.click(screen.getByText(/go-a3/));
    await screen.findByText(/Page A3/);
    expect(screen.getByText(/breadcrumb-a1/)).toBeInTheDocument();
    expect(screen.getByText(/breadcrumb-a2/)).toBeInTheDocument();
    expect(screen.getByText(/breadcrumb-a3/)).toBeInTheDocument();

    await user.click(screen.getByText(/go-b2/));
    await screen.findByText(/Page B2/);
    // No breadcrumbs because only one of the pages is a main nav page
    expect(screen.queryByText(/breadcrumb-b1/)).not.toBeInTheDocument();
    expect(screen.queryByText(/breadcrumb-b3/)).not.toBeInTheDocument();

    await user.click(screen.getByText(/go-b3/));
    await screen.findByText(/Page B3/);
    expect(screen.getByText(/breadcrumb-b1/)).toBeInTheDocument();
    expect(screen.getByText(/breadcrumb-b3/)).toBeInTheDocument();

    // Test that single non-parent pages work as well
    await user.click(screen.getByText(/go-c/));
    await screen.findByText(/Page C/);
    expect(screen.queryByText(/breadcrumb-c/)).not.toBeInTheDocument();
  });
});
