import json
import time
import random
from kafka import KafkaProducer

KAFKA_TOPIC = 'npp-raw-telemetry'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

NPP_CONFIG = [
    {"name": "Zaporizhzhia", "units": 6, "code": "ZNPP"},
    {"name": "Rivne", "units": 4, "code": "RNPP"},
    {"name": "South Ukraine", "units": 3, "code": "SUNPP"},
    {"name": "Khmelnytskyi", "units": 2, "code": "KhNPP"}
]

class ReactorUnit:
    def __init__(self, plant_name, unit_number):
        self.unit_id = f"{plant_name}-{unit_number}"
        self.neutron_flux = 1000.0
        self.power = 1000.0
        self.temp_inlet = 290.0
        self.temp_outlet = 320.0
        self.pressure = 16.0
        self.rod_position = 80.0
        self.radiation = 10.0
        self.state_mode = "NORMAL"
        self.anomaly_counter = 0

    def update(self):
        """Updates the reactor state with some random drift (physics simulation)"""
        
        if self.state_mode == "NORMAL" and random.random() < 0.01:
            self.state_mode = "STRESS"
            self.anomaly_counter = 10 # Lasts for 10 ticks

        if self.state_mode == "STRESS":
            self.pressure += random.uniform(0.1, 0.5) 
            self.neutron_flux += random.uniform(10, 50)
            self.radiation += random.uniform(0.5, 2.0)
            self.anomaly_counter -= 1
            if self.anomaly_counter <= 0:
                self.state_mode = "NORMAL"
        else:
            self.pressure += random.uniform(-0.1, 0.1)
            self.pressure = self.drift_towards(self.pressure, 16.0)
            self.neutron_flux = self.drift_towards(self.neutron_flux, 1000.0)
            self.radiation = self.drift_towards(self.radiation, 10.0)

        self.power = self.neutron_flux * 0.95
        self.temp_inlet += random.uniform(-0.5, 0.5)
        delta_t = (self.power / 30.0)
        self.temp_outlet = self.temp_inlet + delta_t

        self.radiation = max(0.1, self.radiation)

    def drift_towards(self, current, target):
        """Helper to gently drift values back to nominal"""
        diff = target - current
        return current + (diff * 0.1) + random.uniform(-0.05, 0.05)

    def generate_payload(self):
        self.update()
        return {
            "unit_id": self.unit_id,
            "timestamp": int(time.time() * 1000), # Epoch millis
            "neutron_flux": round(self.neutron_flux, 2),
            "reactor_power": round(self.power, 2),
            "coolant_temp_inlet": round(self.temp_inlet, 2),
            "coolant_temp_outlet": round(self.temp_outlet, 2),
            "reactor_pressure": round(self.pressure, 2),
            "control_rod_position": int(self.rod_position),
            "radiation_level": round(self.radiation, 2)
        }

def run_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        return

    reactors = []
    for plant in NPP_CONFIG:
        for i in range(1, plant["units"] + 1):
            reactors.append(ReactorUnit(plant["code"], i))

    print(f"Starting simulation for {len(reactors)} Reactor Units...")

    try:
        count = 0
        while True:
            if count > 10000:
                break

            # start_time = time.time()

            for unit in reactors:
                data = unit.generate_payload()

                producer.send(KAFKA_TOPIC, key=unit.unit_id.encode('utf-8'), value=data)

                count = count + 1
                # print(f"[Sent] {data['unit_id']}")

            producer.flush()
            
            # elapsed = time.time() - start_time
            # sleep_time = max(0, 2.0 - elapsed)
            # time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\nStopping producer...")
        producer.close()

if __name__ == "__main__":
    run_producer()
