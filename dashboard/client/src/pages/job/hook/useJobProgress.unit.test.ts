import { StateApiJobProgressByTaskName, TaskProgress } from "../../../type/job";
import { TypeTaskStatus } from "../../../type/task";
import { formatSummaryToTaskProgress } from "./useJobProgress";

describe("formatSummaryToTaskProgress", () => {
  it("formats correctly", () => {
    const summary: StateApiJobProgressByTaskName = {
      node_id_to_summary: {
        cluster: {
          summary: {
            task_1: {
              func_or_class_name: "task_1",
              state_counts: {
                [TypeTaskStatus.FINISHED]: 1,
                [TypeTaskStatus.FAILED]: 2,
                [TypeTaskStatus.RUNNING]: 3,
                [TypeTaskStatus.RUNNING_IN_RAY_GET]: 4,
                [TypeTaskStatus.PENDING_ARGS_AVAIL]: 5,
              },
            },
            task_2: {
              func_or_class_name: "task_2",
              state_counts: {
                [TypeTaskStatus.FINISHED]: 100,
                [TypeTaskStatus.NIL]: 5,
              },
            },
            task_3: {
              func_or_class_name: "task_3",
              state_counts: {
                [TypeTaskStatus.RUNNING]: 1,
                someNewState: 1,
              },
            },
          },
        },
      },
    };

    const expected: { name: string; progress: TaskProgress }[] = [
      {
        name: "task_1",
        progress: {
          numFinished: 1,
          numFailed: 2,
          numRunning: 7,
          numPendingArgsAvail: 5,
        },
      },
      {
        name: "task_2",
        progress: {
          numFinished: 100,
          numUnknown: 5,
        },
      },
      {
        name: "task_3",
        progress: {
          numRunning: 1,
          numUnknown: 1,
        },
      },
    ];

    expect(formatSummaryToTaskProgress(summary)).toEqual(expected);
  });
});
