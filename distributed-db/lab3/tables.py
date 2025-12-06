from cassandra.cluster import Cluster

cluster = Cluster(['localhost'])
session = cluster.connect()
session.set_keyspace('npp_lab3')

session.execute('''
CREATE TABLE IF NOT EXISTS telemetry_simple (
  device_id text,
  ts timestamp,
  sensor_id text,
  core_temp float,
  pressure float,
  neutron_flux double,
  power_output double,
  status text,
  PRIMARY KEY (device_id, ts)
) WITH CLUSTERING ORDER BY (ts DESC)
  AND gc_grace_seconds = 86400;
''')

session.execute('''
CREATE TABLE IF NOT EXISTS telemetry_hourly (
  device_id text,
  bucket_hour int,
  ts timestamp,
  sensor_id text,
  core_temp float,
  pressure float,
  neutron_flux double,
  power_output double,
  status text,
  PRIMARY KEY ((device_id, bucket_hour), ts)
) WITH CLUSTERING ORDER BY (ts DESC)
  AND gc_grace_seconds = 86400;
''')

session.execute('''
CREATE TABLE IF NOT EXISTS telemetry_daily (
  device_id text,
  bucket_date date,
  ts timestamp,
  sensor_id text,
  core_temp float,
  pressure float,
  neutron_flux double,
  power_output double,
  status text,
  PRIMARY KEY ((device_id, bucket_date), ts)
) WITH CLUSTERING ORDER BY (ts DESC)
  AND gc_grace_seconds = 86400;
''')

session.execute('''
CREATE TABLE IF NOT EXISTS telemetry_daily_agg (
  device_id text,
  bucket_date date,
  metric text,
  count bigint,
  sum double,
  min double,
  max double,
  avg double,
  PRIMARY KEY ((device_id, bucket_date), metric)
);
''')

session.execute("""
CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_simple_mv_power AS
SELECT device_id, ts, power_output, core_temp, pressure, neutron_flux, status
FROM telemetry_simple
WHERE device_id IS NOT NULL
  AND ts IS NOT NULL
  AND power_output IS NOT NULL
PRIMARY KEY ((device_id), power_output, ts)
WITH CLUSTERING ORDER BY (power_output DESC, ts DESC);
""")

session.execute("""
CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_hourly_mv_power AS
SELECT device_id, bucket_hour, ts, power_output, core_temp, pressure, neutron_flux, status
FROM telemetry_hourly
WHERE device_id IS NOT NULL
  AND bucket_hour IS NOT NULL
  AND ts IS NOT NULL
  AND power_output IS NOT NULL
PRIMARY KEY ((device_id, bucket_hour), power_output, ts)
WITH CLUSTERING ORDER BY (power_output DESC, ts DESC);
""")

session.execute("""
CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_daily_mv_power AS
SELECT device_id, bucket_date, ts, power_output, core_temp, pressure, neutron_flux, status
FROM telemetry_daily
WHERE device_id IS NOT NULL
  AND bucket_date IS NOT NULL
  AND ts IS NOT NULL
  AND power_output IS NOT NULL
PRIMARY KEY ((device_id, bucket_date), power_output, ts)
WITH CLUSTERING ORDER BY (power_output DESC, ts DESC);
""")

print("\nTables/views created successfuly.")
cluster.shutdown()
