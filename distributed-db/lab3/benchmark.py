import argparse
import time
import numpy as np
import datetime as dt
from cassandra.cluster import Cluster
from tqdm import tqdm

REACTORS = ['reactor-1', 'reactor-2', 'reactor-3', 'reactor-4']


def percentiles(latencies_ms):
    return {
        'avg': float(sum(latencies_ms)/len(latencies_ms)),
        'p50': float(np.percentile(latencies_ms, 50)),
        'p95': float(np.percentile(latencies_ms, 95)),
        'p99': float(np.percentile(latencies_ms, 99))
    }


def q_latest(session, schema, device_id):
    if schema == 'simple':
        cql = "SELECT * FROM telemetry_simple WHERE device_id = %s LIMIT 100"
        params = (device_id,)
    elif schema == 'hourly':
        now = dt.datetime.now(dt.timezone.utc)
        bh = int(now.replace(minute=0, second=0, microsecond=0).timestamp() // 3600)
        cql = "SELECT * FROM telemetry_hourly WHERE device_id=%s AND bucket_hour=%s LIMIT 100"
        params = (device_id, bh)
    else:
        now = dt.date.today()
        cql = "SELECT * FROM telemetry_daily WHERE device_id=%s AND bucket_date=%s LIMIT 100"
        params = (device_id, now)
    start = time.perf_counter()
    rows = session.execute(cql, params)
    elapsed = (time.perf_counter() - start) * 1000.0
    _ = list(rows)
    return elapsed


def q_time_range(session, schema, device_id, hours=6):
    end = dt.datetime.now(dt.timezone.utc)
    start_ts = end - dt.timedelta(hours=hours)
    if schema == 'simple':
        cql = "SELECT * FROM telemetry_simple WHERE device_id=%s AND ts >= %s AND ts <= %s"
        params = (device_id, start_ts, end)
    elif schema == 'hourly':
        b_end = int(end.replace(minute=0, second=0, microsecond=0).timestamp() // 3600)
        cql = "SELECT * FROM telemetry_hourly WHERE device_id=%s AND bucket_hour=%s AND ts >= %s AND ts <= %s"
        params = (device_id, b_end, start_ts, end)
    else:
        bday = end.date()
        cql = "SELECT * FROM telemetry_daily WHERE device_id=%s AND bucket_date=%s AND ts >= %s AND ts <= %s"
        params = (device_id, bday, start_ts, end)
    start = time.perf_counter()
    rows = session.execute(cql, params)
    elapsed = (time.perf_counter() - start) * 1000.0
    _ = list(rows)
    return elapsed


def q_daily_agg(session, schema, device_id):
    if schema == 'daily':
        cql = "SELECT count, avg, min, max FROM telemetry_daily_agg WHERE device_id=%s AND bucket_date=%s"
        params = (device_id, dt.date.today())
    else:
        if schema == 'hourly':
            cql = "SELECT power_output FROM telemetry_hourly WHERE device_id=%s AND bucket_hour=%s"
            bh = int(dt.datetime.now(dt.timezone.utc).replace(minute=0, second=0, microsecond=0).timestamp() // 3600)
            params = (device_id, bh)
        else:
            cql = "SELECT power_output FROM telemetry_simple WHERE device_id=%s"
            params = (device_id,)
    start = time.perf_counter()
    rows = session.execute(cql, params)
    vals = [r.power_output for r in rows if getattr(r,'power_output',None) is not None]
    elapsed = (time.perf_counter() - start) * 1000.0
    if vals:
        _ = (len(vals), sum(vals)/len(vals), min(vals), max(vals))
    return elapsed


def q_filtered(session, schema, device_id, method='allow_filtering'):
    if method == 'mv':
        if schema == 'hourly':
            bh = int(dt.datetime.now(dt.timezone.utc).replace(minute=0, second=0, microsecond=0).timestamp() // 3600)
            cql = "SELECT * FROM telemetry_hourly_mv_power WHERE device_id=%s AND bucket_hour=%s AND power_output > %s LIMIT 100"
            params = (device_id, bh, 900)
        elif schema == 'daily':
            bd = dt.date.today()
            cql = "SELECT * FROM telemetry_daily_mv_power WHERE device_id=%s AND bucket_date=%s AND power_output > %s LIMIT 100"
            params = (device_id, bd, 900)
        else:
            cql = "SELECT * FROM telemetry_simple_mv_power WHERE device_id=%s AND power_output > %s LIMIT 100"
            params = (device_id, 900)
    else:
        if schema == 'hourly':
            bh = int(dt.datetime.now(dt.timezone.utc).replace(minute=0, second=0, microsecond=0).timestamp() // 3600)
            cql = "SELECT * FROM telemetry_hourly WHERE device_id=%s AND bucket_hour=%s AND power_output > %s ALLOW FILTERING"
            params = (device_id, bh, 900)
        elif schema == 'daily':
            bd = dt.date.today()
            cql = "SELECT * FROM telemetry_daily WHERE device_id=%s AND bucket_date=%s AND power_output > %s ALLOW FILTERING"
            params = (device_id, bd, 900)
        else:
            cql = "SELECT * FROM telemetry_simple WHERE device_id=%s AND power_output > %s ALLOW FILTERING"
            params = (device_id, 900)
    start = time.perf_counter()
    rows = session.execute(cql, params)
    elapsed = (time.perf_counter() - start) * 1000.0
    _ = list(rows)
    return elapsed


def run_one(schema, session, iters):
    results = {'latest':[], 'range6':[], 'daily':[], 'filtered_af':[], 'filtered_mv':[]}
    device = REACTORS[0]
    for _ in tqdm(range(iters), desc=f"Benchmark {schema}"):
        results['latest'].append(q_latest(session, schema, device))
        results['range6'].append(q_time_range(session, schema, device, hours=6))
        results['daily'].append(q_daily_agg(session, schema, device))
        results['filtered_af'].append(q_filtered(session, schema, device, method='allow_filtering'))
        results['filtered_mv'].append(q_filtered(session, schema, device, method='mv'))
    out = {}
    for k,v in results.items():
        out[k] = percentiles(v)
    return out


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--contact-points', nargs='+', default=['localhost'])
    parser.add_argument('--keyspace', default='npp_lab3')
    parser.add_argument('--iters', type=int, default=50)
    args = parser.parse_args()

    cluster = Cluster(args.contact_points)
    session = cluster.connect(args.keyspace)
    schemas = ['simple','hourly','daily']
    import json
    all_results = {}
    for s in schemas:
        out = run_one(s, session, args.iters)
        all_results[s] = out
    print(json.dumps(all_results, indent=2))
    session.shutdown()
    cluster.shutdown()
