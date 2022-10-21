import { render, screen } from "@testing-library/react";
import MockDate from "mockdate";
import React from "react";
import { act } from "react-dom/test-utils";
import { DurationText } from "./DurationText";

describe("DurationText", () => {
  it("renders", async () => {
    const { rerender } = render(
      <DurationText startTime={new Date(100000)} endTime={new Date(105000)} />,
    );

    expect(await screen.findByText("5s")).toBeInTheDocument();
    rerender(
      <DurationText startTime={new Date(100000)} endTime={new Date(110000)} />,
    );
    expect(await screen.findByText("10s")).toBeInTheDocument();
    rerender(
      <DurationText startTime={new Date(100000)} endTime={new Date(200000)} />,
    );
    expect(await screen.findByText("1m 40s")).toBeInTheDocument();
    rerender(
      <DurationText startTime={new Date(100000)} endTime={new Date(5000000)} />,
    );
    expect(await screen.findByText("1h 21m")).toBeInTheDocument();
  });

  it("automatically re-renders when endTime is null", async () => {
    jest.useFakeTimers();

    const startTime = new Date(100000);
    const mockDate1 = new Date(105000);
    const mockDate2 = new Date(106000);
    const mockDate3 = new Date(200000);
    const mockDate4 = new Date(5000000);
    const mockDate5 = new Date(5060000);
    const endTime = new Date(5120000);

    MockDate.set(mockDate1);
    const { rerender } = render(<DurationText startTime={startTime} />);
    expect(await screen.findByText("5s")).toBeInTheDocument();

    MockDate.set(mockDate2);
    act(() => {
      jest.advanceTimersByTime(1000);
    });
    expect(await screen.findByText("6s")).toBeInTheDocument();

    MockDate.set(mockDate3);
    act(() => {
      jest.advanceTimersByTime(1000);
    });
    expect(await screen.findByText("1m 40s")).toBeInTheDocument();

    MockDate.set(mockDate4);
    act(() => {
      jest.advanceTimersByTime(1000);
    });
    expect(await screen.findByText("1h 21m")).toBeInTheDocument();

    MockDate.set(mockDate5);
    // After 1 hr, don't expect dates to re-render every second
    act(() => {
      jest.advanceTimersByTime(1000);
    });
    // date should not have changed
    expect(await screen.findByText("1h 21m")).toBeInTheDocument();
    act(() => {
      jest.advanceTimersByTime(60000);
    });
    expect(await screen.findByText("1h 22m")).toBeInTheDocument();

    rerender(<DurationText startTime={startTime} endTime={endTime} />);
    expect(await screen.findByText("1h 23m")).toBeInTheDocument();

    MockDate.reset();
    jest.runOnlyPendingTimers();
    jest.useRealTimers();
  });
});
