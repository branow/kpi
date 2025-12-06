from cassandra.cluster import Cluster

CASSANDRA_CONTACT_POINTS = ['localhost']
KEYSPACE = 'npp_lab3'

cluster = Cluster(CASSANDRA_CONTACT_POINTS)
session = cluster.connect()

session.execute(
    f"""
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
    WITH replication = {{'class':'NetworkTopologyStrategy','datacenter1':3}}
    """
)
session.set_keyspace(KEYSPACE)

print("\nNamespace created successfuly.")
cluster.shutdown()
