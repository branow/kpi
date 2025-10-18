import json
import os
import signal
import subprocess
import time
from pathlib import Path
from run_utils import run
from typing import Dict, Callable, Any, Optional
from confluent_kafka import Producer, KafkaError, Consumer, TopicPartition


class KafkaClusterManager:
    def __init__(
        self,
        cluster_name: str,
        kafka_dir: str,
        workdir: str,
        nodes: int = 3,
        base_broker_port: int = 9092,
        base_controller_port: int = 9029
    ):
        self.cluster_name = cluster_name
        self.kafka_dir = Path(kafka_dir).expanduser().resolve()
        self.workdir = Path(workdir).expanduser().resolve() / cluster_name
        self.nodes = int(nodes)
        self.base_broker_port = int(base_broker_port)
        self.base_controller_port = int(base_controller_port)
        self.config_dir = self.workdir / 'config'
        self.data_dir = self.workdir / 'data'
        self.logs_dir = self.workdir / 'logs'
        self.proc_info_file = self.workdir / 'processes.json'
        self.cluster_id_file = self.workdir / 'cluster.id'
        self._ensure_dirs()

    def init(self):
        self._write_configs()
        self._format_storage()

    def load_cluster_id(self):
        if self.cluster_id_file.exists():
            self.cluster_id = self.cluster_id_file.read_text().strip()
            return self.cluster_id
        return None

    def start(self) -> bool:
        self._start_nodes()
        ok = self._wait_for_quorum(timeout=60)
        if not ok:
            self._stop_nodes()
        return ok

    def stop(self):
        self._stop_nodes()

    def clean(self):
        # stop then remove workdir
        self._stop_nodes()
        print('Removing workdir', self.workdir)
        for p in self.workdir.glob('*'):
            if p.is_file():
                p.unlink()
            else:
                # naive recursive remove
                for sub in p.rglob('*'):
                    if sub.is_file():
                        sub.unlink()
                try:
                    p.rmdir()
                except Exception:
                    pass
        try:
            self.workdir.rmdir()
        except Exception:
            pass


    def endpoints(self):
        brokers = [f"localhost:{self.base_broker_port + (n-1)}" for n in range(1, self.nodes + 1)]
        controllers = [f"localhost:{self.base_controller_port + (n-1)}" for n in range(1, self.nodes + 1)]
        return {'brokers': brokers, 'controllers': controllers}

    def brokers(self) -> list[str]:
        return self.endpoints()['brokers']

    def brokers_str(self) -> str:
        return ",".join(self.endpoints()['brokers'])

    def status(self):
        print('Workdir:', self.workdir)
        print('Kafka dir:', self.kafka_dir)
        print('Nodes:', self.nodes)
        print('Endpoints:', json.dumps(self.endpoints(), indent=2))
        if self.proc_info_file.exists():
            print('Process info:', self.proc_info_file.read_text())
        else:
            print('No process info (not started with this tool or already stopped).')

    def create_topic(self, topic: str, partitions: int = 1, replication: int = 1):
        bootstrap = self.brokers()[0]
        cmd = [self._kafka_bin('kafka-topics.sh'), '--create', '--topic', topic, '--partitions', str(partitions), '--replication-factor', str(replication), '--bootstrap-server', bootstrap]
        rc, out, err = run(cmd, capture=True, check=False)
        print(out)
        if rc != 0 and err:
            print(f"Create topic '{topic}' rc={rc} err={err}")

    def describe_topic(self, topic: str):
        bootstrap = self.brokers()[0]
        cmd = [
            self._kafka_bin('kafka-topics.sh'),
            '--describe',
            '--topic', topic,
            '--bootstrap-server', bootstrap
        ]
        rc, out, err = run(cmd, capture=True, check=False)
        print(out)
        if err:
            print(f"Describe topic '{topic}' rc={rc} err={err}")

    def create_producer(self) -> Producer:
        conf = {
            "bootstrap.servers": self.brokers_str(),
            "acks": "all",
            "retries": 5,
            "message.timeout.ms": 600_000,
            "delivery.timeout.ms": 600_000,
        }

        producer = Producer(conf)
        return producer

    def send_messages(
        self,
        producer: Producer,
        topic: str,
        data_generator: Callable[[int], dict[Any, Any]],
        num_messages: int,
        flush_every: int = 50
    ) -> None:
        def delivery_report(err: Optional[KafkaError], msg: Any):
            if err is not None:
                print(f"Message delivery failed: {err}")

        for i in range(num_messages):
            record = data_generator(i)
            producer.produce(
                topic=topic,
                value=json.dumps(record).encode("utf-8"),
                callback=delivery_report
            )
            producer.poll(0)

            if (i + 1) % flush_every == 0:
                print(f"{i + 1}/{num_messages} messages queued.")

        producer.flush()
        print("All messages sent successfully.")

    def producer_perf_test(
        self,
        topic: str,
        num_records: int,
        record_size: int,
        compression: str = "none",
        throughput: int = -1,
        batch_size: int = 16384,
        linger_ms: int = 5,
    ):
        props = [
            f"acks=all",
            f"batch.size={batch_size}",
            f"linger.ms={linger_ms}",
            f"compression.type={compression}",
            f"bootstrap.servers={self.brokers_str()}"
        ]
        cmd = [
            self._kafka_bin('kafka-producer-perf-test.sh'),
            '--topic', topic,
            '--num-records', str(num_records),
            '--record-size', str(record_size),
            '--throughput', str(throughput),
            '--producer-props',
            *props
        ]
        rc, out, err = run(cmd, capture=True, check=False)
        print(out)
        if rc != 0 and err:
            print(f"Test producer performance '{topic}' rc={rc} err={err}")

    def topic_partition_size(self, topic: str, partition: int = 0) -> int:
        total_size = 0
        partition_suffix = f"{topic}-{partition}"

        for n in range(1, self.nodes + 1):
            broker_dir = self._node_data_dir(n)
            if not broker_dir.exists():
                continue

            for entry in broker_dir.iterdir():
                if entry.is_dir() and entry.name.startswith(partition_suffix):
                    for log_file in entry.glob('*.log'):
                        total_size += log_file.stat().st_size

        return total_size

    def topic_partition_messages(self, topic: str, partition: int = 0) -> int:
        c = Consumer({
            'bootstrap.servers': self.brokers_str(),
            'group.id': 'dummy',
            'auto.offset.reset': 'earliest'
        })
        tp = TopicPartition(topic, partition)
        low, high = c.get_watermark_offsets(tp)
        return high - low

    def _write_configs(self):
        controller_quorum = self._controller_quorum_voters()
        for n in range(1, self.nodes + 1):
            node_id = n
            bport = self.base_broker_port + (n-1)
            cport = self.base_controller_port + (n-1)
            cfg = {
                'process.roles': 'broker,controller',
                'node.id': str(node_id),
                'controller.listener.names': 'CONTROLLER',
                'controller.quorum.voters': controller_quorum,
                'listeners': f'PLAINTEXT://:{bport},CONTROLLER://:{cport}',
                'listener.security.protocol.map': 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
                'inter.broker.listener.name': 'PLAINTEXT',
                'advertised.listeners': f'PLAINTEXT://localhost:{bport}',
                'log.dirs': str(self._node_data_dir(n)),
                'offsets.topic.replication.factor': '3',
                'transaction.state.log.replication.factor': '3',
                'transaction.state.log.min.isr': '2'
            }
            path = self._node_config_path(n)
            with open(path, 'w') as f:
                for k, v in cfg.items():
                    f.write(f"{k}={v}\n")
        print(f"Wrote {self.nodes} broker config(s) into {self.config_dir}")

    def _format_storage(self):
        # require cluster id
        cid = self.load_cluster_id() or self._generate_cluster_id()
        for n in range(1, self.nodes + 1):
            conf = str(self._node_config_path(n))
            cmd = [self._kafka_bin('kafka-storage.sh'), 'format', '-t', cid, '-c', conf]
            rc, out, err = run(cmd, capture=True, check=False)
            if rc != 0:
                # Show error but continue
                print(f"WARNING: formatting node {n} returned rc={rc}\nstdout={out}\nstderr={err}")
            else:
                print(f"Formatted node {n}")

    def _start_nodes(self):
        procs: Dict[str, object] = {}
        for n in range(1, self.nodes + 1):
            conf = str(self._node_config_path(n))
            logf = open(self._node_log(n), 'ab')
            cmd = [self._kafka_bin('kafka-server-start.sh'), conf]
            print('Starting node', n, '-> log:', self._node_log(n))
            p = subprocess.Popen(cmd, stdout=logf, stderr=subprocess.STDOUT)
            procs[str(n)] = {'pid': p.pid, 'conf': conf, 'log': str(self._node_log(n))}
            # small delay so logs start writing and ports free up sequentially
            time.sleep(0.5)
        self.proc_info_file.write_text(json.dumps(procs, indent=2, sort_keys=True))
        print('Started nodes, process info recorded to', self.proc_info_file)
        return procs

    def _wait_for_quorum(self, timeout: int = 30) -> bool:
        deadline = time.time() + timeout
        controllers = [f"localhost:{self.base_controller_port + (n-1)}" for n in range(1, self.nodes + 1)]
        last_err = None
        while time.time() < deadline:
            for c in controllers:
                cmd = [self._kafka_bin('kafka-metadata-quorum.sh'), '--bootstrap-controller', c, 'describe', '--status']
                rc, out, err = run(cmd, capture=True, check=False)
                if rc == 0 and 'LeaderId' in out or 'Leader' in out:
                    print('Quorum OK via controller', c)
                    return True
                last_err = (rc, out, err)
            time.sleep(1)
        print('Timed out waiting for quorum. Last error:', last_err)
        return False

    def _stop_nodes(self):
        if not self.proc_info_file.exists():
            print('No processes.json found in workdir; nothing to stop (or started manually).')
            return
        procs = json.loads(self.proc_info_file.read_text())
        for k, info in procs.items():
            pid = info.get('pid')
            try:
                print('Stopping node', k, 'pid', pid)
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                print('Process', pid, 'not found')
        # remove processes file
        try:
            self.proc_info_file.unlink()
        except Exception:
            pass

    def _generate_cluster_id(self) -> str:
        # use kafka-storage.sh random-uuid
        cmd = [self._kafka_bin('kafka-storage.sh'), 'random-uuid']
        _, out, _  = run(cmd, capture=True)
        cluster_id = out.strip()
        self.cluster_id = cluster_id
        self.cluster_id_file.write_text(cluster_id)
        return cluster_id

    def _controller_quorum_voters(self):
        parts: list[str] = []
        for n in range(1, self.nodes + 1):
            parts.append(f"{n}@localhost:{self.base_controller_port + (n-1)}")
        return ','.join(parts)

    def _ensure_dirs(self):
        for d in (self.workdir, self.config_dir, self.data_dir, self.logs_dir):
            d.mkdir(parents=True, exist_ok=True)

    def _kafka_bin(self, name: str):
        return str(self.kafka_dir / 'bin' / name)

    def _node_config_path(self, node_idx: int):
        return self.config_dir / f'broker-{node_idx}.properties'

    def _node_log(self, node_idx: int):
        return self.logs_dir / f'broker-{node_idx}.log'

    def _node_data_dir(self, node_idx: int):
        return self.data_dir / f'kraft-combined-logs-{node_idx}'

