#!/usr/bin/env python

from dask_janelia import autoClient
from distributed import progress
from pathlib import Path
import os


def process_file(fname):
    # return the size in bytes of the file
    result = os.path.getsize(fname)
    return result


if __name__ == "__main__":
    with autoClient(threads_per_worker=1) as cl:
        home_files = list(Path.home().glob("*"))
        futures = cl.map(process_file, home_files)
        progress(futures)
        result = cl.gather(futures)

    print(*zip(map(lambda v: str(v.parts[-1]), home_files), result))
