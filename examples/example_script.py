#!/usr/bin/env python

from dask_janelia import autoClient
from distributed import progress, Client
from pathlib import Path
import os
import numpy as np
import time

def process_file(fname):
    # return the size in bytes of the file
    result = os.path.getsize(fname)
    # simulate a long-running process by sleeping for 0-1s
    time.sleep(np.random.random_choice())
    return result


if __name__ == "__main__":
    with autoClient(host="") as cl:
        print(cl.cluster.dashboard_url)
        home_files = list(Path.home().glob("*"))
        futures = cl.map(process_file, home_files)
        progress(futures)
        result = cl.gather(futures)

    print(*zip(map(lambda v: str(v.name), home_files), result))
