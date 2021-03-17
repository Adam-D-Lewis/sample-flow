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

worker_config = {
    "kind": "Pod",
    "metadata": {
        "labels": {
            "foo": "bar"
        }
    },
    "spec": {
        "containers": [
            {
                "args": [
                    "dask-worker",
                    "--nthreads",
                    "1",
                    "--no-dashboard",
                    "--memory-limit",
                    "1GB",
                    "--death-timeout",
                    "60"
                ],
                "env": [
                    {
                        "name": "EXTRA_PIP_PACKAGES",
                        "value": "distributed==2021.3.0"
                    }
                ],
                "image": "daskdev/dask:latest",
                "imagePullPolicy": "IfNotPresent",
                "name": "dask",
                "resources": {
                    "limits": {
                        "cpu": "1",
                        "memory": "1G"
                    },
                    "requests": {
                        "cpu": "1",
                        "memory": "1G"
                    }
                }
            }
        ],
        "restartPolicy": "Never"
    }
}
import subprocess
subprocess.run(['pip', 'install', 'dask-kubernetes==2021.3.0'])
flow.executor = DaskExecutor(cluster_class='dask_kubernetes.KubeCluster', 
                                cluster_kwargs={'pod_template': worker_config},
                                adapt_kwargs={'minimum':2, 'maximum': 3}
                            )
flow.storage = GitHub(
    repo="adam-d-lewis/sample-flow", path="sample_flow.py"
)

if __name__=="__main__":
    flow.register('Test Project', labels=[])  
