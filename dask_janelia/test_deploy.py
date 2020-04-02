from .deploy import bsubAvailable, autoClient, getLSFCluster
from distributed import Client, LocalCluster
from dask_jobqueue import LSFCluster
import time
import pytest
import os

# check if this code is running on the compute cluster
running_on_cluster = bsubAvailable()


@pytest.fixture(params=["lsf", "local-default"], scope="module")
def client(request):
    if request.param == "lsf":
        if running_on_cluster:
            return autoClient(force_local=False)
        else:
            return None
    if request.param == "local":
        return autoClient(force_local=True)


def test_scaling(client, num_workers=1):
    if client is None:
        pytest.skip()
    client.cluster.scale(0)
    time.sleep(1.5)
    client.cluster.scale(num_workers)
    client.wait_for_workers(num_workers)
    assert len(client.cluster.workers) == num_workers
    client.cluster.scale(0)
    time.sleep(0.5)
    assert len(client.cluster.workers) == 0


def test_single_threaded(client):
    if client is None:
        pytest.skip()

    if isinstance(client.cluster, LSFCluster):
        client.cluster.scale(1)
        worker_env = client.submit(_get_env).result()
        client.cluster.scale(0)
        assert worker_env["NUM_MKL_THREADS"] == "1"
        assert worker_env["OPENBLAS_NUM_THREADS"] == "1"
        assert worker_env["OPENMP_NUM_THREADS"] == "1"
        assert worker_env["OMP_NUM_THREADS"] == "1"
    elif isinstance(client.cluster, LocalCluster):
        pytest.skip()


def _get_env():
    import os

    return os.environ
