## **Order of Laboratory Work Execution**

### **1. Environment Setup**

1.1. Start **Zookeeper** and **Kafka Server**.
1.2. Create the **main topic** according to your assigned variant (replace the name and number of partitions as specified in your task).
1.3. Create a **test topic** for experiments with **batch** and **linger** parameters.
1.4. Generate **test data** in **JSON** format according to your variant’s parameters
(for example: `irradiance`, `cloud_factor` for solar panels; `wind_speed`, `rotor_rpm` for wind turbines).

---

### **2. Study of the Effect of `batch.size` and `linger.ms`**

2.1. Run tests for the following combinations:
`batch.size = 16KB, 64KB, 256KB`
`linger.ms = 0, 10, 50`

2.2. Use your **test topic**.
2.3. Make a **conclusion** on how **latency** and **throughput** change depending on these parameters.

---

### **3. Compression Algorithm Testing**

3.1. Run tests using the following algorithms: **none**, **snappy**, **lz4**, **zstd**.
3.2. Create a **separate topic** for each algorithm.
3.3. Compare the resulting **compression ratio** and **latency**.

---

### **4. Study of Partitioning Effects**

4.1. Create three topics with different numbers of partitions: **3**, **6**, and **12**.
4.2. Use **key-based partitioning** (for example, `key = device_id`).
4.3. Make a conclusion about how **increasing the number of partitions** affects **throughput**.

---

### **5. Results Analysis**

5.1. Summarize the results in a **table or graph** (showing latency, throughput, and compression ratio).
5.2. Formulate **recommendations** for different scenarios in your variant:

* **Real-time monitoring** – minimal latency
* **Telemetry** – balance between latency and throughput
* **Archiving** – maximum storage efficiency

---

### **6. Environment Cleanup (if necessary)**

6.1. Stop all **Kafka** and **Zookeeper** processes.
6.2. Delete temporary data (logs).

---

## **My Variant**

### **System Context**

* **Number of devices:** 4 nuclear reactors
* **Transmission frequency:** Each reactor sends data every 5 seconds
* **Geography:** Various regions of Ukraine (coordinates: 47.0–52.0°N, 30.0–36.0°E)
* **Characteristics:** Critical safety, maximum reliability, strict monitoring

---

### **Data Structure (256 bytes)**

```
device_id: "REACTOR_ZA_1" to "REACTOR_RV_2";
power_output: 800.0–1000.0 MW (reactor power)
efficiency: 32.0–35.0% (unit efficiency)
temperature: 285.0–295.0°C (coolant temperature)
voltage: 21,000.0–23,000.0 V (generator voltage)
current: 20,000.0–28,000.0 A (current)
status: "normal", "maintenance", "startup", "shutdown"
location: lat 47.0–52.0, lon 30.0–36.0
maintenance_hours: 1000–8760 hours until scheduled maintenance
neutron_flux: 90.0–100.0% (neutron flux level)
pressure: 150.0–160.0 atm (primary circuit pressure)
rod_position: "75pct", "85pct", "95pct" (control rod position)
```

---

### **Testing Parameters**

* **Maximum reliability:** `acks=all`, `replication_factor=3`
* **Target throughput:** 100 records/sec (fewer devices, critical quality)
* **Focus:** fault tolerance and consistency

---

## **Execution Stages**

### **Stage 1: Preparation (0–15 min)**

* Create topics with maximum reliability:
  `nuclear-main` (3 partitions, replication=3)
* Generate **500 high-quality test records** with reactor parameters
* Configure `acks=all` and **high timeout values** for reliability

---

### **Stage 2: Batch/Linger Testing (15–40 min)**

* Test combinations:
  `batch_sizes = (16384, 65536, 262144)` × `linger_ms = (0, 10, 50)`
* Topic: `nuclear-batch-test`
* **Critical neutron_flux alerts:** `linger.ms=0` is mandatory
* Expected results: **latency < 10ms** for critical parameters
* Goal: maximum delivery reliability with `acks=all`

---

### **Stage 3: Compression Testing (40–60 min)**

* Algorithms: **none**, **snappy**, **lz4**, **zstd**
* Topics:
  `nuclear-comp-none`, `nuclear-comp-snappy`, `nuclear-comp-lz4`, `nuclear-comp-zstd`
* Reactor temperature data is highly stable → excellent compression potential
* **Priority:** reliability > compression efficiency
* **Compression = none** for critical alerts is mandatory
* Expected compression ratios:
  `snappy = 35–45%`, `lz4 = 40–50%`, `zstd = 50–60%`

---

### **Stage 4: Partitioning (60–75 min)**

* Test configurations: 3, 6, 9 partitions (with `replication_factor=3`)
* Topics: `nuclear-part-3`, `nuclear-part-6`, `nuclear-part-9`
* Strategy: partition by `reactor_id` for safe isolation
* Expected scaling: focus on reliability, not throughput
* Each partition has 3 replicas for fault tolerance

---

### **Stage 5: Analysis and Recommendations (75–90 min)**

* **Critical safety alerts:**
  `batch.size=16KB`, `linger.ms=0`, `compression=none`, `acks=all`
* **Normal telemetry:**
  `batch.size=32KB`, `linger.ms=5`, `compression=snappy`
* **Historical data:**
  `batch.size=256KB`, `linger.ms=30`, `compression=zstd`
* Generate a **report** including neutron_flux safety alerts and system reliability results

---

### **Success Criteria**

* **Latency <10ms** for neutron_flux and pressure alerts
* **100% delivery guarantee** for all safety-critical messages
* **Fault tolerance** when 1 out of 3 brokers fails
