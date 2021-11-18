from ray import workflow
workflow.init()


@workflow.step
def id_step(x):
    return x


@workflow.step
def x_step():
    id_step.step(10).run("ABC")


x_step.step().run()
