from cassandra.cluster import Cluster


cluster = Cluster(['localhost'])
session = cluster.connect()
if cluster.metadata is not None:
    print('Connected to cluster:', cluster.metadata.cluster_name)
    if cluster.metadata.keyspaces is not None:
        print('Keyspaces:', [ks for ks in cluster.metadata.keyspaces])
    cluster.shutdown()
