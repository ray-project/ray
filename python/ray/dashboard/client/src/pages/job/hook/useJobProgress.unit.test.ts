import {
  JobProgressGroup,
  StateApiJobProgressByTaskName,
  StateApiNestedJobProgress,
  TaskProgress,
} from "../../../type/job";
import { TypeTaskStatus, TypeTaskType } from "../../../type/task";
import {
  formatNestedJobProgressToJobProgressGroup,
  formatSummaryToTaskProgress,
} from "./useJobProgress";

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

describe("formatNestedJobProgressToJobProgressGroup", () => {
  it("formats correctly without", () => {
    // First without hiding progress
    const summary: StateApiNestedJobProgress = {
      node_id_to_summary: {
        cluster: {
          summary: [
            {
              name: "failed_tasks",
              key: "failed_tasks_group",
              state_counts: {
                [TypeTaskStatus.FAILED]: 3,
              },
              type: "GROUP",
              children: [
                {
                  name: "failed_tasks",
                  key: "failed_tasks_1",
                  state_counts: {
                    [TypeTaskStatus.FAILED]: 1,
                  },
                  type: TypeTaskType.NORMAL_TASK,
                  children: [],
                },
                {
                  name: "failed_tasks",
                  key: "failed_tasks_2",
                  state_counts: {
                    [TypeTaskStatus.FAILED]: 1,
                  },
                  type: TypeTaskType.NORMAL_TASK,
                  children: [],
                },
                {
                  name: "failed_tasks",
                  key: "failed_tasks_3",
                  state_counts: {
                    [TypeTaskStatus.FAILED]: 1,
                  },
                  type: TypeTaskType.NORMAL_TASK,
                  children: [],
                },
              ],
            },
            {
              name: "succeeded_tasks",
              key: "succeeded_tasks_group",
              state_counts: {
                [TypeTaskStatus.FINISHED]: 2,
              },
              type: "GROUP",
              children: [
                {
                  name: "succeeded_tasks",
                  key: "succeeded_tasks_1",
                  state_counts: {
                    [TypeTaskStatus.FINISHED]: 1,
                  },
                  type: TypeTaskType.NORMAL_TASK,
                  children: [],
                },
                {
                  name: "succeeded_tasks",
                  key: "succeeded_tasks_2",
                  state_counts: {
                    [TypeTaskStatus.FINISHED]: 1,
                  },
                  type: TypeTaskType.NORMAL_TASK,
                  children: [],
                },
              ],
            },
            {
              name: "father_task",
              key: "father_task",
              state_counts: {
                [TypeTaskStatus.FINISHED]: 1,
                [TypeTaskStatus.FAILED]: 1,
              },
              type: TypeTaskType.NORMAL_TASK,
              children: [
                {
                  name: "suceeded_child",
                  key: "succeeded_child_1",
                  state_counts: {
                    [TypeTaskStatus.FINISHED]: 1,
                  },
                  type: TypeTaskType.NORMAL_TASK,
                  children: [],
                },
                {
                  name: "failed_child",
                  key: "failed_child_1",
                  state_counts: {
                    [TypeTaskStatus.FAILED]: 1,
                  },
                  type: TypeTaskType.NORMAL_TASK,
                  children: [],
                },
              ],
            },
          ],
        },
      },
    };

    const expectedWithFinishedTasks: {
      total: TaskProgress;
      progressGroups: JobProgressGroup[];
    } = {
      total: {
        numFinished: 3,
        numFailed: 4,
      },
      progressGroups: [
        {
          name: "failed_tasks",
          key: "failed_tasks_group",
          progress: {
            numFailed: 3,
          },
          link: undefined,
          type: "GROUP",
          children: [
            {
              name: "failed_tasks",
              key: "failed_tasks_1",
              progress: {
                numFailed: 1,
              },
              link: undefined,
              type: TypeTaskType.NORMAL_TASK,
              children: [],
            },
            {
              name: "failed_tasks",
              key: "failed_tasks_2",
              progress: {
                numFailed: 1,
              },
              link: undefined,
              type: TypeTaskType.NORMAL_TASK,
              children: [],
            },
            {
              name: "failed_tasks",
              key: "failed_tasks_3",
              progress: {
                numFailed: 1,
              },
              link: undefined,
              type: TypeTaskType.NORMAL_TASK,
              children: [],
            },
          ],
        },
        {
          name: "succeeded_tasks",
          key: "succeeded_tasks_group",
          progress: {
            numFinished: 2,
          },
          link: undefined,
          type: "GROUP",
          children: [
            {
              name: "succeeded_tasks",
              key: "succeeded_tasks_1",
              progress: {
                numFinished: 1,
              },
              link: undefined,
              type: TypeTaskType.NORMAL_TASK,
              children: [],
            },
            {
              name: "succeeded_tasks",
              key: "succeeded_tasks_2",
              progress: {
                numFinished: 1,
              },
              link: undefined,
              type: TypeTaskType.NORMAL_TASK,
              children: [],
            },
          ],
        },
        {
          name: "father_task",
          key: "father_task",
          progress: {
            numFinished: 1,
            numFailed: 1,
          },
          link: undefined,
          type: TypeTaskType.NORMAL_TASK,
          children: [
            {
              name: "suceeded_child",
              key: "succeeded_child_1",
              progress: {
                numFinished: 1,
              },
              link: undefined,
              type: TypeTaskType.NORMAL_TASK,
              children: [],
            },
            {
              name: "failed_child",
              key: "failed_child_1",
              progress: {
                numFailed: 1,
              },
              link: undefined,
              type: TypeTaskType.NORMAL_TASK,
              children: [],
            },
          ],
        },
      ],
    };

    expect(formatNestedJobProgressToJobProgressGroup(summary, true)).toEqual(
      expectedWithFinishedTasks,
    );

    const expectedWithoutFinishedTasks: {
      total: TaskProgress;
      progressGroups: JobProgressGroup[];
    } = {
      total: {
        numFinished: 3,
        numFailed: 4,
      },
      progressGroups: [
        {
          name: "failed_tasks",
          key: "failed_tasks_group",
          type: "GROUP",
          progress: {
            numFailed: 3,
          },
          children: [
            {
              key: "failed_tasks_1",
              name: "failed_tasks",
              link: undefined,
              type: TypeTaskType.NORMAL_TASK,
              progress: {
                numFailed: 1,
              },
              children: [],
            },
            {
              key: "failed_tasks_2",
              name: "failed_tasks",
              link: undefined,
              type: TypeTaskType.NORMAL_TASK,
              progress: {
                numFailed: 1,
              },
              children: [],
            },
            {
              key: "failed_tasks_3",
              name: "failed_tasks",
              link: undefined,
              type: TypeTaskType.NORMAL_TASK,
              progress: {
                numFailed: 1,
              },
              children: [],
            },
          ],
        },
        {
          name: "father_task",
          key: "father_task",
          link: undefined,
          progress: {
            numFinished: 1,
            numFailed: 1,
          },
          type: TypeTaskType.NORMAL_TASK,
          children: [
            {
              name: "failed_child",
              key: "failed_child_1",
              link: undefined,
              progress: {
                numFailed: 1,
              },
              type: TypeTaskType.NORMAL_TASK,
              children: [],
            },
          ],
        },
      ],
    };

    expect(formatNestedJobProgressToJobProgressGroup(summary, false)).toEqual(
      expectedWithoutFinishedTasks,
    );
  });
});
