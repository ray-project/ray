import { render } from "@testing-library/react";
import React from "react";
import { useServeApplications } from "../../serve/hook/useServeApplications";
import { RecentServeCard } from "./RecentServeCard";

jest.mock("../../serve/hook/useServeApplications");
const mockedUseServeApplications = jest.mocked(useServeApplications);
mockedUseServeApplications.mockReturnValue({
  allServeApplications: [],
} as any);

describe("RecentServeCard", () => {
  it("renders correctly with empty Serve Applications", () => {
    const { getByText } = render(<RecentServeCard />);
    expect(getByText("Recent Applications")).toBeInTheDocument();
    expect(getByText("No Applications yet...")).toBeInTheDocument();
    expect(getByText("View all applications")).toHaveAttribute(
      "href",
      "/serve",
    );
  });
});
