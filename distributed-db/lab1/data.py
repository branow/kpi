import random, time

reactors = ["REACTOR_ZA_1", "REACTOR_ZA_2", "REACTOR_RV_1", "REACTOR_RV_2"]

def generate_record(i: int = 0):
    reactor = random.choice(reactors)
    return {
        "device_id": reactor,
        "power_output": round(random.uniform(800.0, 1000.0), 2),
        "efficiency": round(random.uniform(32.0, 35.0), 2),
        "temperature": round(random.uniform(285.0, 295.0), 2),
        "voltage": round(random.uniform(21000.0, 23000.0), 2),
        "current": round(random.uniform(20000.0, 28000.0), 2),
        "status": random.choice(["normal", "maintenance", "startup", "shutdown"]),
        "location": {
            "lat": round(random.uniform(47.0, 52.0), 4),
            "lon": round(random.uniform(30.0, 36.0), 4)
        },
        "maintenance_hours": random.randint(1000, 8760),
        "neutron_flux": round(random.uniform(90.0, 100.0), 2),
        "pressure": round(random.uniform(150.0, 160.0), 2),
        "rod_position": random.choice(["75pct", "85pct", "95pct"]),
        "timestamp": time.time()
    }
