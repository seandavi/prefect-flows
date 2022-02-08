from prefect.run_configs import KubernetesRun

import prefect
from prefect import task, Flow
from prefect.storage import GitHub


@task
def hello_task():
    logger = prefect.context.get("logger")
    logger.info("Hello world!")

with Flow("hello-flow") as flow:
    hello_task()


# Configure extra environment variables for this flow,
# and set a custom image
flow.run_config = KubernetesRun(
    env={"SOME_VAR": "VALUE"},
    image="gcr.io/omicidx-338300/prefect-gcp-base"
)

flow.storage = GitHub(repo='seandavi/prefect-flows', path='prefect-test-kubernetes/flow.py')

flow.register('testing')

