CREATE KEYSPACE npp_lab4 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE npp_lab4;

CREATE TABLE nuclear_event_log (
    unit_id text,
    event_time timestamp,
    event_type text,
    payload text,
    PRIMARY KEY (unit_id, event_time)
) WITH CLUSTERING ORDER BY (event_time DESC);

CREATE TABLE reactor_state (
    unit_id text,
    window_start timestamp,
    avg_neutron_flux double,
    thermal_efficiency double,
    safety_margin double,
    PRIMARY KEY (unit_id, window_start)
);
