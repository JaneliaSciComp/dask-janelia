from dask_janelia import get_cluster
from dask_janelia.deploy import bsub_available
from distributed import Client
from dask_jobqueue import LSFCluster
import time
import pytest

# check if this code is running on the compute cluster
running_on_cluster = bsub_available()


@pytest.fixture(
    params=[("lsf", 1), ("local", 1), ("lsf", 2), ("local", 2)], scope="module"
)
def cluster(request):
    deployment: str = request.param[0]
    threads_per_worker: int = request.param[1]
    if deployment == "lsf":
        if running_on_cluster:
            return get_cluster(
                deployment=deployment, threads_per_worker=threads_per_worker
            )
        else:
            return None
    if request.param == "local":
        return get_cluster(deployment=deployment, threads_per_worker=threads_per_worker)


@pytest.mark.parametrize("num_workers", [1, 2, 3])
def test_scaling(cluster, num_workers):
    if not cluster:
        pytest.skip()
    client = Client(cluster)
    client.cluster.scale(0)
    time.sleep(1.5)
    client.cluster.scale(num_workers)
    client.wait_for_workers(num_workers)
    # seems that client.wait_for_workers can return control before the dict of workers
    # is updated...
    time.sleep(0.2)
    assert len(client.cluster.workers) == num_workers
    client.cluster.scale(0)
    time.sleep(0.5)
    assert len(client.cluster.workers) == 0


def test_threading_env_vars(cluster):
    if not cluster:
        pytest.skip()

    def _get_env():
        import os

        return os.environ

    if isinstance(cluster, LSFCluster):
        client = Client(cluster)
        client.cluster.scale(1)
        client.wait_for_workers(1)
        threads_per_worker = [
            spec["options"]["cores"] for spec in client.cluster.worker_spec.values()
        ][0]
        worker_env = client.submit(_get_env).result()
        client.cluster.scale(0)
        assert worker_env["NUM_MKL_THREADS"] == str(threads_per_worker)
        assert worker_env["OPENBLAS_NUM_THREADS"] == str(threads_per_worker)
        assert worker_env["OPENMP_NUM_THREADS"] == str(threads_per_worker)
        assert worker_env["OMP_NUM_THREADS"] == str(threads_per_worker)
