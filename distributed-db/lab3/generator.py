import argparse
import time
import random
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from cassandra.cluster import Cluster
from cassandra.query import BatchType, BatchStatement
from cassandra import ConsistencyLevel


def make_session(contact_points, keyspace):
    cluster = Cluster(contact_points)
    session = cluster.connect()
    try:
        session.set_keyspace(keyspace)
    except Exception:
        print(f"[error] Keyspace '{keyspace}' not found. Please create it first.")
        exit(1)
    return session


def prepare_statements(session, schema):
    if schema == 'simple':
        query = """
            INSERT INTO telemetry_simple
            (device_id, ts, sensor_id, core_temp, pressure, neutron_flux, power_output, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""
    elif schema == 'hourly':
        query = """
            INSERT INTO telemetry_hourly
            (device_id, bucket_hour, ts, sensor_id, core_temp, pressure, neutron_flux, power_output, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    elif schema == 'daily':
        query = """
            INSERT INTO telemetry_daily
            (device_id, bucket_date, ts, sensor_id, core_temp, pressure, neutron_flux, power_output, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    else:
        raise ValueError(f"Unknown schema '{schema}'")

    ps = session.prepare(query)
    ps.consistency_level = ConsistencyLevel.ONE
    return ps


def generate_rows_for_device(start_dt, end_dt, interval_seconds):
    ts = start_dt
    sensor_ids = ['sensor_core', 'sensor_pressure', 'sensor_flux', 'sensor_power']

    while ts < end_dt:
        core_temp = random.uniform(280.0, 340.0)          # °C
        pressure = random.uniform(60.0, 80.0)             # atm
        neutron_flux = random.uniform(1e12, 5e13)         # n/cm2*s
        power_output = random.uniform(600.0, 1000.0)      # MWe
        status = 'OK' if random.random() > 0.001 else 'ALARM'
        sensor_id = random.choice(sensor_ids)
        yield (ts, sensor_id, core_temp, pressure, neutron_flux, power_output, status)
        ts += timedelta(seconds=interval_seconds)


def chunked(iterable, n):
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= n:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def insert_rows(session, schema, device_id, start_dt, end_dt, interval_seconds, batch_size):
    ps = prepare_statements(session, schema)
    inserted = 0

    for chunk in chunked(generate_rows_for_device(start_dt, end_dt, interval_seconds), batch_size):
        batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        for (ts, sensor_id, core_temp, pressure, neutron_flux, power_output, status) in chunk:
            if schema == 'simple':
                batch.add(ps, (device_id, ts, sensor_id, core_temp, pressure, neutron_flux, power_output, status))
            elif schema == 'hourly':
                bucket_hour = int(ts.replace(tzinfo=timezone.utc).timestamp() // 3600)
                batch.add(ps, (device_id, bucket_hour, ts, sensor_id, core_temp, pressure, neutron_flux, power_output, status))
            elif schema == 'daily':
                bucket_date = ts.date()
                batch.add(ps, (device_id, bucket_date, ts, sensor_id, core_temp, pressure, neutron_flux, power_output, status))
        session.execute(batch)
        inserted += len(chunk)

    print(f"[done] device={device_id}, inserted={inserted}")
    return inserted


def main():
    parser = argparse.ArgumentParser(description="Generate and insert nuclear plant telemetry data into Cassandra")
    parser.add_argument('--schema', choices=['simple', 'hourly', 'daily'], default='hourly', help="Schema variant to use")
    parser.add_argument('--contact-points', nargs='+', default=['localhost'])
    parser.add_argument('--keyspace', default='npp_lab3')
    parser.add_argument('--devices', type=int, default=4)
    parser.add_argument('--days', type=int, default=30)
    parser.add_argument('--interval-seconds', type=int, default=10)
    parser.add_argument('--batch-size', type=int, default=50)
    parser.add_argument('--workers', type=int, default=8)
    args = parser.parse_args()

    session = make_session(args.contact_points, args.keyspace)

    devices = [f'reactor_{i+1}' for i in range(args.devices)]
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=args.days)

    total_expected = int((args.days * 24 * 3600 / args.interval_seconds) * args.devices)
    print(f"[info] Generating ~{total_expected:,} rows across {args.devices} devices...")
    print(f"[info] Schema={args.schema}, batch_size={args.batch_size}, workers={args.workers}")

    start = time.perf_counter()
    total_inserted = 0
    futures = []

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        for device_id in devices:
            f = executor.submit(
                insert_rows,
                session,
                args.schema,
                device_id,
                start_dt,
                end_dt,
                args.interval_seconds,
                args.batch_size
            )
            futures.append(f)

        for f in as_completed(futures):
            total_inserted += f.result()

    elapsed = time.perf_counter() - start
    print(f"\nInserted total {total_inserted:,} rows in {elapsed:.1f}s "
          f"({total_inserted/elapsed:.1f} rows/sec)")

    session.shutdown()


if __name__ == "__main__":
    main()
