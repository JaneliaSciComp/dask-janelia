#!/usr/bin/env python

from dask_janelia import auto_cluster
from distributed import progress, Client
from pathlib import Path
import os
import random
import time


def process_file(fname):
    # return the size in bytes of the file
    result = os.path.getsize(fname)
    # simulate a long-running process by sleeping for a few seconds
    time.sleep(5 * random.random())
    return result


if __name__ == "__main__":
    # wrapping the client in a context manager ensures that the client and the cluster get cleaned up
    with Client(auto_cluster()) as cl:
        print(f"Cluster dashboard running at {cl.cluster.dashboard_link}")
        # list all the files in the home directory
        home_files = list(Path.home().glob("*"))
        # map the function `process_file` over `home_files`
        futures = cl.map(process_file, home_files)
        progress(futures)
        # get the results
        result = cl.gather(futures)

    print(*zip(map(lambda v: str(v.name), home_files), result))
