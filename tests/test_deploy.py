from dask_janelia.deploy import bsub_available, auto_cluster, JaneliaCluster
from distributed import Client, LocalCluster
from dask_jobqueue import LSFCluster
import time
import pytest

# check if this code is running on the compute cluster
running_on_cluster = bsub_available()


@pytest.fixture(params=["lsf", "local"], scope="module")
def cluster(request):
    if request.param == "lsf":
        if running_on_cluster:
            return auto_cluster(local=False)
        else:
            return None
    if request.param == "local":
        return auto_cluster(local=True)


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


def test_single_threaded(cluster):
    if not cluster:
        pytest.skip()

    client = Client(cluster)

    if isinstance(cluster, LSFCluster):
        client.cluster.scale(1)
        worker_env = client.submit(_get_env).result()
        client.cluster.scale(0)
        assert worker_env["NUM_MKL_THREADS"] == "1"
        assert worker_env["OPENBLAS_NUM_THREADS"] == "1"
        assert worker_env["OPENMP_NUM_THREADS"] == "1"
        assert worker_env["OMP_NUM_THREADS"] == "1"
    elif isinstance(cluster, LocalCluster):
        pytest.skip()


def _get_env():
    import os

    return os.environ
