import { Table, TableBody } from "@material-ui/core";
import { ThemeProvider } from "@material-ui/styles";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React, { PropsWithChildren } from "react";
import { lightTheme } from "../../../theme";
import { TypeTaskType } from "../../../type/task";
import { AdvancedProgressBarSegment } from "./AdvancedProgressBar";

const Wrapper = ({ children }: PropsWithChildren<{}>) => {
  return (
    <ThemeProvider theme={lightTheme}>
      <Table>
        <TableBody>{children}</TableBody>
      </Table>
    </ThemeProvider>
  );
};

describe("AdvancedProgressBarSegment", () => {
  it("renders without children", async () => {
    expect.assertions(2);
    render(
      <AdvancedProgressBarSegment
        jobProgressGroup={{
          name: "group 1",
          key: "group1",
          progress: {
            numFinished: 1,
            numRunning: 9,
          },
          children: [],
          type: TypeTaskType.ACTOR_CREATION_TASK,
        }}
      />,
      { wrapper: Wrapper },
    );
    await screen.findByText(/group 1/);
    expect(screen.getByText(/1 \/ 10/)).toBeVisible();
    expect(screen.getByTitle("Expand").parentElement).not.toBeVisible();
  });

  it("renders with children", async () => {
    expect.assertions(7);
    const user = userEvent.setup();

    render(
      <AdvancedProgressBarSegment
        jobProgressGroup={{
          name: "group 1",
          key: "group1",
          progress: {
            numFinished: 1,
            numRunning: 9,
          },
          children: [
            {
              name: "child",
              key: "child",
              progress: {
                numFinished: 1,
              },
              children: [],
              type: TypeTaskType.NORMAL_TASK,
            },
          ],
          type: TypeTaskType.ACTOR_CREATION_TASK,
        }}
      />,
      { wrapper: Wrapper },
    );
    await screen.findByText(/group 1/);
    expect(screen.getByTitle("Expand").parentElement).toBeVisible();
    expect(screen.getByText(/^1 \/ 10$/)).toBeVisible();
    await user.click(screen.getByTitle("Expand"));
    await screen.findByText(/child/);
    screen.getByText(/child/);
    expect(screen.getByTitle("Collapse").parentElement).toBeVisible();
    expect(screen.getAllByTitle("Expand")).toHaveLength(1); // There should only be one for the child segment
    expect(screen.getByText(/^1 \/ 1$/)).toBeVisible();
    await user.click(screen.getByTitle("Collapse"));
    expect(screen.queryByText(/child/)).toBeNull();
    expect(screen.queryByText(/^1 \/ 1$/)).toBeNull();
  });

  it("renders with GROUP and children", async () => {
    expect.assertions(12);
    const user = userEvent.setup();

    render(
      <AdvancedProgressBarSegment
        jobProgressGroup={{
          name: "group 1",
          key: "group1",
          progress: {
            numFinished: 3,
            numRunning: 7,
          },
          type: "GROUP",
          children: [
            {
              name: "child",
              key: "child",
              progress: {
                numFinished: 3,
              },
              type: TypeTaskType.NORMAL_TASK,
              children: [
                {
                  name: "grandchild",
                  key: "grandchild",
                  progress: {
                    numFinished: 1,
                  },
                  type: TypeTaskType.NORMAL_TASK,
                  children: [],
                },
              ],
            },
          ],
        }}
      />,
      { wrapper: Wrapper },
    );
    await screen.findByText(/group 1/);
    expect(screen.getByTitle("Expand").parentElement).toBeVisible();
    expect(screen.getByText(/^3 \/ 10$/)).toBeVisible();
    await user.click(screen.getByTitle("Expand"));
    await screen.findByText(/child/);
    screen.getByText(/child/);
    expect(screen.getByTitle("Collapse group").parentElement).toBeVisible();
    expect(screen.getAllByTitle("Expand")).toHaveLength(1); // There should only be one for the child segment
    expect(screen.getByText(/^3 \/ 3$/)).toBeVisible();
    await user.click(screen.getByTitle("Expand"));
    await screen.findByText(/grandchild/);
    expect(screen.getByTitle("Collapse group").parentElement).toBeVisible();
    expect(screen.getByTitle("Collapse").parentElement).toBeVisible(); // Collapse on the child segment
    expect(screen.getAllByTitle("Expand")).toHaveLength(1); // There should only be one for the grand child segment
    expect(screen.getByText(/^1 \/ 1$/)).toBeVisible();
    await user.click(screen.getByTitle("Collapse group"));
    expect(screen.getByText(/^3 \/ 10$/)).toBeVisible();
    expect(screen.queryByText(/^3 \/ 3$/)).toBeNull();
    expect(screen.queryByText(/^1 \/ 1$/)).toBeNull();
  });
});
