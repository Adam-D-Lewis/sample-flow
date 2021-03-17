from prefect import task, Flow
from prefect.executors import DaskExecutor
from prefect.storage import GitHub
import datetime
import random

import yaml

@task
def inc(x):
    return x + 1


@task
def dec(x):
    return x - 1


@task
def add(x, y):
    return x + y


@task
def list_sum(arr):
    return sum(arr)

with Flow("test-flow") as flow:
    incs = inc.map(x=range(100))
    decs = dec.map(x=range(100))
    adds = add.map(x=incs, y=decs)
    total = list_sum(adds)

if __name__=="__main__":
    with open('dask-worker-spec.yml') as f:
        worker_config = yaml.load(f, Loader=yaml.FullLoader)

    flow.executor = DaskExecutor(cluster_class='dask_kubernetes.KubeCluster', 
                                  cluster_kwargs={'pod_template': worker_config},
                                  adapt_kwargs={'minimum':2, 'maximum': 3}
                                )
    flow.storage = GitHub(
        repo="adam-d-lewis/sample-flow", path="sample_flow.py"
    )
    flow.register('Test Project', labels=[])  
