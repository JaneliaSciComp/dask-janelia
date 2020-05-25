#!/usr/bin/env python

from dask_janelia import autoClient
from distributed import progress, Client
from pathlib import Path
import os
import random
import time


def process_file(fname):
    # return the size in bytes of the file
    result = os.path.getsize(fname)
    # simulate a long-running process by sleeping for 0-1s
    time.sleep(random.random())
    return result


if __name__ == "__main__":
    with autoClient(cluster_kwargs={"host": ""}) as cl:
        print(f"Cluster dashboard running at {cl.cluster.dashboard_link}")
        home_files = list(Path.home().glob("*"))
        futures = cl.map(process_file, home_files)
        progress(futures)
        result = cl.gather(futures)

    print(*zip(map(lambda v: str(v.name), home_files), result))
