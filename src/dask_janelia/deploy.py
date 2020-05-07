from shutil import which
from distributed import Client, LocalCluster
from dask_jobqueue import LSFCluster
import os
from pathlib import Path
import warnings


def JaneliaCluster(
    queue: str = "normal",
    walltime: str = "1:00",
    cores: int = 1,
    memory: str = "16GB",
    threads_per_worker: int = 1,
    **kwargs,
) -> LSFCluster:
    """Create a dask_jobqueue.LSFCluster for use on the Janelia Research Campus compute cluster.

    This function wraps the class dask_jobqueue.LSFCLuster and instantiates this class with some sensible defaults.
    Additional keyword arguments added to this function will be passed to the LSFCluster constructor.

    Parameters
    ----------
    queue: str
        The name of the LSF queue to submit to. Defaults to "normal".
    walltime: str
        The expected lifetime of a worker. Defaults to one hour, i.e. "1:00"
    cores: int
        The number of CPUs to request per worker. Defaults to 1.
    memory: str
        The amount of memory to request per worker. Defaults to 16 GB.
    threads_per_worker: int
        If this value is 1, then extra environment variables are set on the worker to guard against running multithreaded code on the workers.
        No action is taken if this value is not 1.
        This kwarg is named to match a corresponding kwarg in the LocalCluster constructor.

    Examples
    --------

    >>> cluster = JaneliaCluster(cores=2, memory="32GB", project="scicompsoft")

    """

    if "env_extra" not in kwargs:
        kwargs["env_extra"] = []

    if threads_per_worker == 1:
        if cores > 1:
            warnings.warn(
                """
            You have requested multiple cores per worker, but set threads_per_worker to 1. Your workers may not be able to run multithreaded
            libraries.
            """
            )
        # Set environment variables to prevent worker code from running multithreaded.
        kwargs["env_extra"].extend(
            [
                "export NUM_MKL_THREADS=1",
                "export OPENBLAS_NUM_THREADS=1",
                "export OPENMP_NUM_THREADS=1",
                "export OMP_NUM_THREADS=1",
            ]
        )
    else:
        warnings.warn(
            f"You have set threads_per_worker to {threads_per_worker}. This parameter only has an effect when set to 1."
        )

    USER = os.environ["USER"]
    HOME = os.environ["HOME"]

    if "local_directory" not in kwargs:
        # The default local scratch directory on the Janelia Cluster
        kwargs["local_directory"] = f"/scratch/{USER}/"

    if "log_directory" not in kwargs:
        log_dir = f"{HOME}/.dask_distributed/"
        Path(log_dir).mkdir(parents=False, exist_ok=True)
        kwargs["log_directory"] = log_dir

    cluster = LSFCluster(walltime=walltime, cores=cores, memory=memory, **kwargs)
    return cluster


def bsubAvailable() -> bool:
    """Check if the `bsub` shell command is available

    Returns True if the `bsub` command is available on the path, False otherwise. This is used to check whether code is
    running on the Janelia Compute Cluster.
    """
    result = which("bsub") is not None
    return result


def autoClient(
    local: bool = False,
    set_as_default: bool = False,
    cluster_kwargs: dict = {},
    **kwargs,
) -> Client:
    """Convenience function to generate a distributed.Client

    Create a distributed.Client object backed by either a dask_jobqueue.LSFCluster (for use on the Janelia Compute Cluster)
    or a distributed.LocalCluster (for use on a single machine). This function uses the output of the bsubAvailable function
    to determine whether code is running on the compute cluster or not.
    Additional keyword arguments given to this function will be forwarded to the constructor for the Client object.

    Parameters
    ----------
    local: bool
        Determines whether to force use of the LocalCluster. Otherwise, calling autoClient in code running on
        the Janelia compute cluster will use LSFCluster. Defaults to False.

    set_as_default: bool
        Determines whether the resulting Client object registers itself as the global default.

    cluster_kwargs: dict
        Dictionary of keyword arguments that will be passed to JaneliaCluster or LocalCluster.
    """
    if bsubAvailable() and not local:
        cluster = JaneliaCluster(**cluster_kwargs)
    else:
        # Allows remote access to the scheduler dashboard
        if "host" not in cluster_kwargs:
            cluster_kwargs["host"] = ""
        cluster = LocalCluster(**cluster_kwargs)

    if "set_as_default" not in kwargs:
        kwargs["set_as_default"] = set_as_default

    client = Client(address=cluster, **kwargs)
    return client
