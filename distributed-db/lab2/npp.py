import random
from datetime import datetime, timedelta, date, timezone
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, BatchStatement

CASSANDRA_CONTACT_POINTS = ['localhost']
KEYSPACE = 'npp_ukraine'
STATION_NAME = 'Zaporizhzhia_NPP'
REACTORS = [
    {'reactor_id': 'R1', 'type': 'VVER-1000'},
    {'reactor_id': 'R2', 'type': 'VVER-1000'},
    {'reactor_id': 'R3', 'type': 'VVER-1000'},
    {'reactor_id': 'R4', 'type': 'VVER-1000'},
]

READINGS_PER_REACTOR = 200

cluster = Cluster(CASSANDRA_CONTACT_POINTS)
session = cluster.connect()

session.execute(
    f"""
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
    WITH replication = {{'class':'SimpleStrategy','replication_factor':'1'}}
    """
)
session.set_keyspace(KEYSPACE)

session.execute('''
CREATE TABLE IF NOT EXISTS reactor_readings (
    reactor_id text,
    reading_time timestamp,
    core_temp float,
    primary_pressure float,
    power_mw float,
    radiation_msv float,
    sensor_status text,
    PRIMARY KEY ((reactor_id), reading_time)
) WITH CLUSTERING ORDER BY (reading_time DESC);
''')

session.execute('''
CREATE TABLE IF NOT EXISTS cooling_system (
    reactor_id text,
    reading_time timestamp,
    pump_id int,
    flow_rate_m3s float,
    inlet_temp_c float,
    outlet_temp_c float,
    pump_status text,
    PRIMARY KEY ((reactor_id), reading_time, pump_id)
) WITH CLUSTERING ORDER BY (reading_time DESC, pump_id ASC);
''')

session.execute('''
CREATE TABLE IF NOT EXISTS daily_safety_summary (
    station_name text,
    summary_date date,
    reactor_id text,
    max_core_temp float,
    max_pressure float,
    avg_radiation float,
    safety_events_count int,
    notes text,
    PRIMARY KEY ((station_name, summary_date), reactor_id)
) WITH CLUSTERING ORDER BY (reactor_id ASC);
''')

session.execute('''
CREATE TABLE IF NOT EXISTS analytics_by_reactor_type (
    reactor_type text,
    period_start date,
    period_end date,
    avg_power_mw float,
    max_radiation float,
    avg_core_temp float,
    num_readings int,
    PRIMARY KEY ((reactor_type), period_start)
) WITH CLUSTERING ORDER BY (period_start ASC);
''')

print("Tables created/verified.\n")

insert_reactor = session.prepare('''
INSERT INTO reactor_readings (reactor_id, reading_time, core_temp, primary_pressure, power_mw, radiation_msv, sensor_status)
VALUES (?, ?, ?, ?, ?, ?, ?)
''')

insert_cooling = session.prepare('''
INSERT INTO cooling_system (reactor_id, reading_time, pump_id, flow_rate_m3s, inlet_temp_c, outlet_temp_c, pump_status)
VALUES (?, ?, ?, ?, ?, ?, ?)
''')

insert_daily = session.prepare('''
INSERT INTO daily_safety_summary (station_name, summary_date, reactor_id, max_core_temp, max_pressure, avg_radiation, safety_events_count, notes)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
''')

insert_analytics = session.prepare('''
INSERT INTO analytics_by_reactor_type (reactor_type, period_start, period_end, avg_power_mw, max_radiation, avg_core_temp, num_readings)
VALUES (?, ?, ?, ?, ?, ?, ?)
''')

def generate_reactor_reading(base_temp=290.0):
    """Simulate a reactor sensor reading."""
    return {
        'core_temp': round(random.uniform(base_temp - 5, base_temp + 8), 2),
        'primary_pressure': round(random.uniform(150.0, 170.0), 2),
        'power_mw': round(random.uniform(900.0, 1000.0), 2),
        'radiation_msv': round(random.uniform(0.01, 0.05), 4),
        'sensor_status': random.choice(['OK', 'OK', 'OK', 'WARN'])
    }

def generate_cooling_reading():
    """Simulate a cooling system telemetry record."""
    return {
        'flow_rate_m3s': round(random.uniform(2.5, 4.0), 3),
        'inlet_temp_c': round(random.uniform(25.0, 35.0), 2),
        'outlet_temp_c': round(random.uniform(45.0, 55.0), 2),
        'pump_status': random.choice(['RUNNING', 'RUNNING', 'STOPPED'])
    }

print("Generating and inserting data...")
now = datetime.now(timezone.utc)
start_of_day = datetime(now.year, now.month, now.day)

for reactor in REACTORS:
    reactor_id = reactor['reactor_id']
    base_temp = 290.0

    batch = BatchStatement()
    for i in range(READINGS_PER_REACTOR):
        t = start_of_day + timedelta(seconds=int(i * (86400 / READINGS_PER_REACTOR)))
        t += timedelta(seconds=random.randint(0, 300))
        r = generate_reactor_reading(base_temp)

        batch.add(insert_reactor, (reactor_id, t, r['core_temp'], r['primary_pressure'],
                                   r['power_mw'], r['radiation_msv'], r['sensor_status']))

        for pump_id in [1, 2]:
            c = generate_cooling_reading()
            batch.add(insert_cooling, (reactor_id, t, pump_id, c['flow_rate_m3s'],
                                       c['inlet_temp_c'], c['outlet_temp_c'], c['pump_status']))

        if len(batch) >= 50:
            session.execute(batch)
            batch.clear()

    if len(batch) > 0:
        session.execute(batch)

    max_core_temp = round(base_temp + random.uniform(3.0, 8.0), 2)
    max_pressure = round(random.uniform(160.0, 175.0), 2)
    avg_radiation = round(random.uniform(0.015, 0.035), 4)
    safety_events_count = random.randint(0, 2)
    notes = '' if safety_events_count == 0 else 'Minor alarm(s) logged'

    session.execute(insert_daily, (STATION_NAME, date.today(), reactor_id, max_core_temp,
                                   max_pressure, avg_radiation, safety_events_count, notes))

week_start = date.today() - timedelta(days=date.today().weekday())
week_end = week_start + timedelta(days=6)
for typ in set(r['type'] for r in REACTORS):
    avg_power_mw = round(random.uniform(920.0, 980.0), 2)
    max_rad = round(random.uniform(0.02, 0.05), 4)
    avg_core_temp = round(random.uniform(288.0, 295.0), 2)
    num_readings = READINGS_PER_REACTOR * len([r for r in REACTORS if r['type'] == typ])
    session.execute(insert_analytics, (typ, week_start, week_end, avg_power_mw, max_rad, avg_core_temp, num_readings))

print("Data inserted successfully.\n")

print("--- Basic Analysis ---")

for reactor in REACTORS:
    reactor_id = reactor['reactor_id']
    stmt = SimpleStatement("SELECT count(*) FROM reactor_readings WHERE reactor_id = %s")
    row = session.execute(stmt, (reactor_id,)).one()
    print(f"Reactor {reactor_id}: {row.count} readings")

total_power = 0
count = 0
for reactor in REACTORS:
    rows = session.execute("SELECT power_mw FROM reactor_readings WHERE reactor_id = %s LIMIT 100", (reactor['reactor_id'],))
    for r in rows:
        total_power += r.power_mw
        count += 1

avg_power = round(total_power / count, 2) if count else None
print(f"\nAverage power across reactors (sample of 100 each): {avg_power} MW")

print("\nLatest readings per reactor:")
for reactor in REACTORS:
    reactor_id = reactor['reactor_id']
    latest = session.execute(
        "SELECT reading_time, core_temp, primary_pressure, power_mw, radiation_msv "
        "FROM reactor_readings WHERE reactor_id = %s LIMIT 1",
        (reactor_id,)
    ).one()
    if latest:
        print(f"  {reactor_id} @ {latest.reading_time}: {latest.power_mw} MW, "
              f"{latest.core_temp} °C, {latest.radiation_msv} mSv")

print("\nAnalysis complete. Program finished successfully.")
cluster.shutdown()
