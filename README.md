# dask-janelia
Dask deployment for the Janelia Compute Cluster

# Usage


```python
from dask_janelia import get_cluster
from distributed import Client

# this returns either an instance of `distributed.LocalCluster` or `dask_jobqueue.LSFCluster`,  
# depending on whether code is running on the janelia compute cluster or not
cluster = get_cluster()
cl = Client(cluster)
```

Be default, an instance of `dask_jobqueue.LSFCluster` with sensible defaults (1 thread / core per worker, 16 GB of ram per worker on LSF) is returned when this code is running on the Janelia compute cluster (i.e., when the `bsub` command is detected in the environment). Otherwise an instance of `distributed.LocalCluster` is returned. 

The automatic detection can be partially overrriden by supplying a keyword argument to `get_cluster`:

```python
# n.b. this is only useful for running a LocalCluster on the compute cluster.
cluster = get_cluster(deployment='local')
```

Under the hood, `get_cluster` is simply calling the `LSFCluster` and `LocalCluster` constructors, which unfortunately have very divergent interfaces. Instead of doing the Right Thing and formalizing an interface that abstracts over both these `*Cluster` classes, `get_cluster` takes two dict arguments called `lsf_kwargs` and `local_kwargs` that are forwarded to the respective class constructors:

```python
# Janelia Cluster: request more memory per job
cluster = get_cluster(lsf_kwargs={'mem': '32GB', 'project' : 'foo'})
```
Consult the documentation for [LocalCluster](https://docs.dask.org/en/latest/setup/single-distributed.html#distributed.deploy.local.LocalCluster) and [LSFCluster](https://jobqueue.dask.org/en/latest/generated/dask_jobqueue.LSFCluster.html#dask_jobqueue.LSFCluster) for more information about what goes in `lsf_kwargs` and `local_kwargs`.
