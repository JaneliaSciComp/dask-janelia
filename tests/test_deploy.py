from dask_janelia import get_cluster
from dask_janelia.deploy import bsub_available
from distributed import Client
from dask_jobqueue import LSFCluster
import time
import pytest

# check if this code is running on the compute cluster
running_on_cluster = bsub_available()


@pytest.mark.parametrize("deployment", ("lsf", "local"))
@pytest.mark.parametrize("num_workers", (1, 2, 3))
def test_scaling(deployment: str, num_workers: int):
    if (deployment == "lsf") and not running_on_cluster:
        pytest.skip()

    with get_cluster(deployment=deployment) as cluster, Client(cluster) as cl:
        assert len(cl.cluster.workers) == 0
        cl.cluster.scale(num_workers)
        cl.wait_for_workers(num_workers)
        # seems that client.wait_for_workers can return control before the dict of workers
        # is updated...
        time.sleep(0.2)
        assert len(cl.cluster.workers) == num_workers
        cl.cluster.scale(0)
        time.sleep(0.5)
        assert len(cl.cluster.workers) == 0


@pytest.mark.parametrize("deployment", ("lsf",))
@pytest.mark.parametrize("threads_per_worker", (1, 2, 3))
def test_threading_env_vars(deployment: str, threads_per_worker: int):
    if (deployment == "lsf") and not running_on_cluster:
        pytest.skip()

    def _get_env():
        import os

        return os.environ

    with get_cluster(
        threads_per_worker=threads_per_worker, deployment=deployment
    ) as clust, Client(clust) as cl:
        cl.cluster.scale(1)
        cl.wait_for_workers(1)
        worker_env = cl.submit(_get_env).result()
        assert worker_env["NUM_MKL_THREADS"] == str(threads_per_worker)
        assert worker_env["OPENBLAS_NUM_THREADS"] == str(threads_per_worker)
        assert worker_env["OPENMP_NUM_THREADS"] == str(threads_per_worker)
        assert worker_env["OMP_NUM_THREADS"] == str(threads_per_worker)
