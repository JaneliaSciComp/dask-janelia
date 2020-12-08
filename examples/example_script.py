#!/usr/bin/env python

from dask_janelia import get_cluster
from distributed import progress, Client
from pathlib import Path
import os
import random
import time

num_workers = 2

mean_runtime = 20


def process_file(fname, delay):
    # return the size in bytes of the file
    result = os.path.getsize(fname)
    # simulate a long-running process by sleeping
    time.sleep(delay * random.random())
    return result


if __name__ == "__main__":
    # wrapping the client in a context manager ensures that the client and the cluster get cleaned up
    with get_cluster() as clust, Client(clust) as cl:
        print(f"Cluster dashboard running at {cl.cluster.dashboard_link}")
        # add workers
        cl.cluster.scale(num_workers)

        # list all the files in the home directory
        home_files = list(Path.home().glob("*"))

        # ensure that the total runtime is ~mean_runtime
        delays = ((mean_runtime * num_workers) / len(home_files),) * len(home_files)

        # map the function `process_file` over the arguments `home_files` and `delays`
        # this returns a collection of futures
        futures = cl.map(process_file, home_files, delays)
        progress(futures)
        # block until all the futures are finished
        result = cl.gather(futures)

    print(*zip(map(lambda v: str(v.name), home_files), result))
