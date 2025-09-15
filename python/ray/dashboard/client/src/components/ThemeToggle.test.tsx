import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { ThemeProvider } from "../contexts/ThemeContext";
import { STYLE_WRAPPER } from "../util/test-utils";
import { ThemeToggle } from "./ThemeToggle";

describe("ThemeToggle", () => {
  const renderWithProviders = () => {
    return render(
      <STYLE_WRAPPER>
        <ThemeProvider>
          <ThemeToggle />
        </ThemeProvider>
      </STYLE_WRAPPER>,
    );
  };

  beforeEach(() => {
    localStorage.clear();
  });

  it("should render moon icon in light mode", () => {
    renderWithProviders();

    const button = screen.getByRole("button");
    expect(button).toBeInTheDocument();

    // Check tooltip text
    expect(screen.getByLabelText("Switch to dark mode")).toBeInTheDocument();
  });

  it("should toggle theme when clicked", async () => {
    const user = userEvent.setup();
    renderWithProviders();

    const button = screen.getByRole("button");

    // Initially should show "Switch to dark mode"
    expect(screen.getByLabelText("Switch to dark mode")).toBeInTheDocument();

    // Click to toggle
    await user.click(button);

    // After toggle, should show "Switch to light mode"
    expect(screen.getByLabelText("Switch to light mode")).toBeInTheDocument();
  });

  it("should persist theme change", async () => {
    const user = userEvent.setup();
    renderWithProviders();

    const button = screen.getByRole("button");

    // Click to toggle to dark mode
    await user.click(button);

    // Check localStorage was updated
    expect(localStorage.getItem("ray-dashboard-theme")).toBe("dark");
  });
});
