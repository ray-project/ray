import ray
from ray import workflow

workflow.init()

@workflow.step
def identity(v):
    if v == 5:
        raise Exception()
    return v

@workflow.step
def w(i):
    s = 0
    for v in range(0, i):
        ww = identity.step(v)
        try:
            p = workflow.get(ww)
            s += p
        except Exception as e:
            print("get an error", v)
            pass
    return s

print(">>>>", w.options().step(10).run())


"""
(base) yic@ip-172-31-58-40:~/upstream-ray/ray/python/dynamic-workflow-examples (dynamic-checkpoint) $ python test.py
2022-03-25 04:32:47,221 INFO services.py:1456 -- View the Ray dashboard at http://127.0.0.1:8265
2022-03-25 04:32:48,940 INFO workflow_access.py:399 -- Initializing workflow manager...
2022-03-25 04:32:50,390 INFO execution.py:58 -- Workflow job created. [id="5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573", storage_url="file:///tmp/ray/workflow_data"]. Type: FUNCTION.
(WorkflowManagementActor pid=10993) 2022-03-25 04:32:50,403     INFO workflow_access.py:193 -- run_or_resume: 5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573, __main__.w,ObjectRef(f4938ab98ac415b8ffffffffffffffffffffffff0100000001000000)
(WorkflowManagementActor pid=10993) 2022-03-25 04:32:50,404     INFO workflow_access.py:204 -- Workflow job [id=5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573] started.
(_workflow_step_executor_remote pid=10470) 2022-03-25 04:32:50,494      INFO step_executor.py:375 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.w]
(_workflow_step_executor_remote pid=10470) 2022-03-25 04:32:50,495      INFO step_executor.py:391 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.w]  [1/3]
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,558      INFO step_executor.py:375 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity]
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,560      INFO step_executor.py:391 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity]   [1/3]
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,562      INFO step_executor.py:568 -- Step status [SUCCESSFUL]   [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity]
(_workflow_step_executor_remote pid=10470) 2022-03-25 04:32:50,561      INFO step_executor.py:568 -- Step status [SUCCESSFUL]   [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.w]
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,663      INFO step_executor.py:375 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_1]
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,663      INFO step_executor.py:391 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_1] [1/3]
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,665      INFO step_executor.py:568 -- Step status [SUCCESSFUL]   [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_1]
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,684      INFO step_executor.py:375 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_2]
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,684      INFO step_executor.py:391 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_2] [1/3]
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,686      INFO step_executor.py:568 -- Step status [SUCCESSFUL]   [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_2]
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,708      INFO step_executor.py:375 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@ray.workflow.step_executor.continuation_3]
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,708      INFO step_executor.py:391 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@ray.workflow.step_executor.continuation_3]   [1/3]
(_workflow_step_executor_remote pid=10470) 2022-03-25 04:32:50,667      INFO step_executor.py:375 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@ray.workflow.step_executor.continuation_1]
(_workflow_step_executor_remote pid=10470) 2022-03-25 04:32:50,667      INFO step_executor.py:391 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@ray.workflow.step_executor.continuation_1]   [1/3]
(_workflow_step_executor_remote pid=10470) 2022-03-25 04:32:50,686      INFO step_executor.py:568 -- Step status [SUCCESSFUL]   [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@ray.workflow.step_executor.continuation_1]
(_workflow_step_executor_remote pid=10470) 2022-03-25 04:32:50,705      INFO step_executor.py:375 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_3]
(_workflow_step_executor_remote pid=10470) 2022-03-25 04:32:50,705      INFO step_executor.py:391 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_3] [1/3]
(_workflow_step_executor_remote pid=10470) 2022-03-25 04:32:50,706      INFO step_executor.py:568 -- Step status [SUCCESSFUL]   [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_3]
(_workflow_step_executor_remote pid=10474) 2022-03-25 04:32:50,640      INFO step_executor.py:375 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@ray.workflow.step_executor.continuation]
(_workflow_step_executor_remote pid=10474) 2022-03-25 04:32:50,640      INFO step_executor.py:391 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@ray.workflow.step_executor.continuation]     [1/3]
(_workflow_step_executor_remote pid=10474) 2022-03-25 04:32:50,663      INFO step_executor.py:568 -- Step status [SUCCESSFUL]   [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@ray.workflow.step_executor.continuation]
(_workflow_step_executor_remote pid=10474) 2022-03-25 04:32:50,687      INFO step_executor.py:375 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@ray.workflow.step_executor.continuation_2]
(_workflow_step_executor_remote pid=10474) 2022-03-25 04:32:50,687      INFO step_executor.py:391 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@ray.workflow.step_executor.continuation_2]   [1/3]
(_workflow_step_executor_remote pid=10474) 2022-03-25 04:32:50,707      INFO step_executor.py:568 -- Step status [SUCCESSFUL]   [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@ray.workflow.step_executor.continuation_2]
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,728      INFO step_executor.py:568 -- Step status [SUCCESSFUL]   [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@ray.workflow.step_executor.continuation_3]
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,747      INFO step_executor.py:375 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_5]
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,747      INFO step_executor.py:391 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_5] [1/3]
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,747      ERROR step_executor.py:416 -- 5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_5 failed with error message . The step will be retried.
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,748      INFO step_executor.py:382 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_5] retries: [1/3]
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,748      INFO step_executor.py:391 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_5] [2/3]
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,748      ERROR step_executor.py:416 -- 5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_5 failed with error message . The step will be retried.
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,748      INFO step_executor.py:382 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_5] retries: [2/3]
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,748      INFO step_executor.py:391 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_5] [3/3]
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,748      ERROR step_executor.py:416 -- 5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_5 failed with error message . The step will be retried.
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,748      INFO step_executor.py:382 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_5] retries: [3/3]
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,748      INFO step_executor.py:391 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_5] [4/3]
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,748      ERROR step_executor.py:416 -- 5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_5 failed with error message . Maximum retry reached, stop retry.
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,750      INFO step_executor.py:568 -- Step status [SUCCESSFUL]   [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_5]
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,770      INFO step_executor.py:375 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_6]
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,770      INFO step_executor.py:391 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_6] [1/3]
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,772      INFO step_executor.py:568 -- Step status [SUCCESSFUL]   [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_6]
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,797      INFO step_executor.py:375 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@ray.workflow.step_executor.continuation_7]
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,797      INFO step_executor.py:391 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@ray.workflow.step_executor.continuation_7]   [1/3]
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,815      INFO step_executor.py:568 -- Step status [SUCCESSFUL]   [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@ray.workflow.step_executor.continuation_7]
(_workflow_step_executor_remote pid=10470) 2022-03-25 04:32:50,730      INFO step_executor.py:375 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@ray.workflow.step_executor.continuation_4]
(_workflow_step_executor_remote pid=10470) 2022-03-25 04:32:50,730      INFO step_executor.py:391 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@ray.workflow.step_executor.continuation_4]   [1/3]
(_workflow_step_executor_remote pid=10470) 2022-03-25 04:32:50,749      INFO step_executor.py:568 -- Step status [SUCCESSFUL]   [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@ray.workflow.step_executor.continuation_4]
(_workflow_step_executor_remote pid=10470) 2022-03-25 04:32:50,774      INFO step_executor.py:375 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@ray.workflow.step_executor.continuation_6]
(_workflow_step_executor_remote pid=10470) 2022-03-25 04:32:50,775      INFO step_executor.py:391 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@ray.workflow.step_executor.continuation_6]   [1/3]
(_workflow_step_executor_remote pid=10470) 2022-03-25 04:32:50,796      INFO step_executor.py:568 -- Step status [SUCCESSFUL]   [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@ray.workflow.step_executor.continuation_6]
(_workflow_step_executor_remote pid=10470) 2022-03-25 04:32:50,813      INFO step_executor.py:375 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_8]
(_workflow_step_executor_remote pid=10470) 2022-03-25 04:32:50,813      INFO step_executor.py:391 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_8] [1/3]
(_workflow_step_executor_remote pid=10470) 2022-03-25 04:32:50,815      INFO step_executor.py:568 -- Step status [SUCCESSFUL]   [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_8]
(_workflow_step_executor_remote pid=10474) 2022-03-25 04:32:50,726      INFO step_executor.py:375 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_4]
(_workflow_step_executor_remote pid=10474) 2022-03-25 04:32:50,726      INFO step_executor.py:391 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_4] [1/3]
(_workflow_step_executor_remote pid=10474) 2022-03-25 04:32:50,728      INFO step_executor.py:568 -- Step status [SUCCESSFUL]   [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_4]
(_workflow_step_executor_remote pid=10474) 2022-03-25 04:32:50,752      INFO step_executor.py:375 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@ray.workflow.step_executor.continuation_5]
(_workflow_step_executor_remote pid=10474) 2022-03-25 04:32:50,752      INFO step_executor.py:391 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@ray.workflow.step_executor.continuation_5]   [1/3]
(_workflow_step_executor_remote pid=10474) 2022-03-25 04:32:50,772      INFO step_executor.py:568 -- Step status [SUCCESSFUL]   [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@ray.workflow.step_executor.continuation_5]
(_workflow_step_executor_remote pid=10474) 2022-03-25 04:32:50,794      INFO step_executor.py:375 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_7]
(_workflow_step_executor_remote pid=10474) 2022-03-25 04:32:50,794      INFO step_executor.py:391 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_7] [1/3]
(_workflow_step_executor_remote pid=10474) 2022-03-25 04:32:50,796      INFO step_executor.py:568 -- Step status [SUCCESSFUL]   [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_7]
(_workflow_step_executor_remote pid=10474) 2022-03-25 04:32:50,816      INFO step_executor.py:375 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@ray.workflow.step_executor.continuation_8]
(_workflow_step_executor_remote pid=10474) 2022-03-25 04:32:50,816      INFO step_executor.py:391 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@ray.workflow.step_executor.continuation_8]   [1/3]
(_workflow_step_executor_remote pid=10474) get an error 5
>>>> 40
(WorkflowManagementActor pid=10993) 2022-03-25 04:32:50,840     INFO workflow_access.py:382 -- Workflow job [id=5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573] succeeded.
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,833      INFO step_executor.py:375 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_9]
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,833      INFO step_executor.py:391 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_9] [1/3]
(_workflow_step_executor_remote pid=10479) 2022-03-25 04:32:50,835      INFO step_executor.py:568 -- Step status [SUCCESSFUL]   [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@__main__.identity_9]
(_workflow_step_executor_remote pid=10470) 2022-03-25 04:32:50,836      INFO step_executor.py:375 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@ray.workflow.step_executor.continuation_9]
(_workflow_step_executor_remote pid=10470) 2022-03-25 04:32:50,836      INFO step_executor.py:391 -- Step status [RUNNING]      [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@ray.workflow.step_executor.continuation_9]   [1/3]
(_workflow_step_executor_remote pid=10470) 2022-03-25 04:32:50,839      INFO step_executor.py:568 -- Step status [SUCCESSFUL]   [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@ray.workflow.step_executor.continuation_9]
(_workflow_step_executor_remote pid=10474) 2022-03-25 04:32:50,835      INFO step_executor.py:568 -- Step status [SUCCESSFUL]   [5b327d99-f767-4acd-a769-f5fa0769888d.1648182770.390134573@ray.workflow.step_executor.continuation_8]
"""
