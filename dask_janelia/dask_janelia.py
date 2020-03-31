#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  Boilerplate for setting up a dask.distributed environment on the janelia compute cluster.
#
# Davis Bennett
# davis.v.bennett@gmail.com
#
# License: MIT
#

from shutil import which
from distributed import Client, LocalCluster
from dask_jobqueue import LSFCluster
import os
from pathlib import Path

def getLSFCluster(
    queue: str ="normal",
    walltime: str = "1:00",
    ncpus: int = 1,
    cores: int = 1,
    memory: str = "16GB",
    threads_per_worker: int = 1,
    **kwargs
) -> dask_jobqueue.LSFCLuster:
    """
    Instantiate a dask_jobqueue cluster using the LSF scheduler on the Janelia Research Campus compute cluster.
    This function wraps the class dask_jobqueue.LSFCLuster and instantiates this class with some sensible defaults.
    Extra kwargs added to this function will be passed to LSFCluster().
    The full API for the LSFCluster object can be found here:
    https://jobqueue.dask.org/en/latest/generated/dask_jobqueue.LSFCluster.html#dask_jobqueue.LSFCluster
    """

    # Set environment variables to prevent worker code from running multithreaded.
    if threads_per_worker == 1:
        env_extra = [
            "export NUM_MKL_THREADS=1",
            "export OPENBLAS_NUM_THREADS=1",
            "export OPENMP_NUM_THREADS=1",
            "export OMP_NUM_THREADS=1",
        ]
    else:
        raise ValueError('threads_per_worker can only be 1')

    USER = os.environ["USER"]
    HOME = os.environ["HOME"]

    if "local_directory" not in kwargs:
        kwargs["local_directory"] = f"/scratch/{USER}/"

    if "log_directory" not in kwargs:
        log_dir = f"{HOME}/.dask_distributed/"
        Path(log_dir).mkdir(parents=False, exist_ok=True)
        kwargs["log_directory"] = log_dir

    cluster = LSFCluster(
        walltime=walltime,
        cores=cores,
        ncpus=ncpus,
        memory=memory,
        env_extra=env_extra,
        **kwargs
    )
    return cluster


def bsubAvailable() -> bool:
    """

    Returns True if the `bsub` command is available on the path, False otherwise. This is used to check whether code is
    running on the Janelia Compute Cluster.

    -------

    """
    result = which("bsub") is not None
    return result


def getCluster(**kwargs) -> distributed.Client:
    """
    Create a distributed.Client object backed by either a dask_jobqueue.LSFCluster (for use on the Janelia Compute Cluster)
    or a distributed.LocalCluster (for use on a single machine). This function uses the output of the bsubAvailable function
    to determine whether code is running on the compute cluster or not.

    Keyword arguments given to this function will be forwarded to the constructor for the cluster object.
    """
    if bsubAvailable():
        cluster = getLSFCluster(**kwargs)
    else:
        if 'host' not in kwargs:
            kwargs['host'] = ''
        cluster = LocalCluster(**kwargs)

    client = Client(cluster, set_as_default=False)
    return client
