from dask_janelia import get_cluster
from dask_janelia.deploy import bsub_available
from distributed import Client
from dask_jobqueue import LSFCluster
import time
import pytest

# check if this code is running on the compute cluster
running_on_cluster = bsub_available()


@pytest.fixture(params=["lsf", "local"], scope="module")
def cluster(request):
    if request.param == "lsf":
        if running_on_cluster:
            return get_cluster(target_deployment="lsf")
        else:
            return None
    if request.param == "local":
        return get_cluster(target_deployment="local")


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


@pytest.mark.parametrize("num_workers", [1, 2, 3])
def test_threading_env_vars(cluster, num_workers):
    if not cluster:
        pytest.skip()

    if isinstance(cluster, LSFCluster):
        client = Client(cluster)
        client.cluster.scale(num_workers)
        worker_env = client.submit(_get_env).result()
        client.cluster.scale(0)
        assert worker_env["NUM_MKL_THREADS"] == str(num_workers)
        assert worker_env["OPENBLAS_NUM_THREADS"] == str(num_workers)
        assert worker_env["OPENMP_NUM_THREADS"] == str(num_workers)
        assert worker_env["OMP_NUM_THREADS"] == str(num_workers)


def _get_env():
    import os

    return os.environ
