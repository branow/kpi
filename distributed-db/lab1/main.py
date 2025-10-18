import argparse
import sys
from data import generate_record
from kafka import KafkaClusterManager


def main():
    parser = argparse.ArgumentParser(description='Manage a local Kafka cluster')
    sub = parser.add_subparsers(dest='cmd')

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument('--kafka-dir', required=True, help='Path to Kafka installation (root dir with bin/)')
    common.add_argument('--workdir', required=True, help='Working directory for configs/data/logs')
    common.add_argument('--cluster-name', required=True, help='Name of your Kafka cluster')

    sub.add_parser('init', parents=[common], help='Generate configs and format storage')
    sub.add_parser('start', parents=[common], help='Start nodes using generated configs')
    sub.add_parser('stop', parents=[common], help='Stop nodes started by this manager')
    sub.add_parser('status', parents=[common], help='Show cluster status and endpoints')
    sub.add_parser('clean', parents=[common], help='Stop and clean (only auto-deletes if workdir in /tmp or /var)')
    sub.add_parser('stg1', parents=[common], help='Stage 1: topic creation')
    sub.add_parser('stg2', parents=[common], help='Stage 2: testing performance with different batch sizes')
    sub.add_parser('stg3', parents=[common], help='Stage 3: testing performance with different compression algorithms')
    sub.add_parser('stg4', parents=[common], help='Stage 4: testing partitioning')

    args = parser.parse_args()
    if not args.cmd:
        parser.print_help()
        sys.exit(1)

    kcm = KafkaClusterManager(
        cluster_name=args.cluster_name,
        kafka_dir=args.kafka_dir,
        workdir=args.workdir,
    )

    if args.cmd == 'init':
        kcm.init()
        print('Cluster id:', kcm.load_cluster_id())
        print('Endpoints:', kcm.endpoints())

    elif args.cmd == 'start':
        kcm.start()

    elif args.cmd == 'stop':
        kcm.stop()

    elif args.cmd == 'status':
        kcm.status()

    elif args.cmd == 'clean':
        kcm.clean()

    elif args.cmd == 'stg1':
        kcm.init()
        if kcm.start():
            topic = 'nuclear-main'
            kcm.create_topic(topic=topic, partitions=3, replication=3)
            kcm.describe_topic(topic=topic)
            producer = kcm.create_producer()
            kcm.send_messages(
                producer=producer,
                topic=topic,
                data_generator=generate_record,
                num_messages=500,
            )
            kcm.stop()
            kcm.clean()


    elif args.cmd == 'stg2':
        kcm.init()
        if kcm.start():
            topic = 'nuclear-batch-test'
            kcm.create_topic(topic=topic, partitions=3, replication=3)
            kcm.describe_topic(topic=topic)

            batch_sizes = [16_384, 65_536, 262_144]
            linger_ms_values = [0, 10, 50]

            for batch_size in batch_sizes:
                for linger_ms in linger_ms_values:
                    print(f"\n=== Testing batch_size={batch_size}, linger_ms={linger_ms} ===\n")
                    kcm.producer_perf_test(
                        topic=topic,
                        num_records=500,
                        record_size=256,
                        batch_size=batch_size,
                        linger_ms=linger_ms
                    )
            kcm.stop()
            kcm.clean()

    elif args.cmd == 'stg3':
        kcm.init()
        if kcm.start():
            compression_algorithms = ["none", "snappy", "lz4", "zstd"]
            for algo in compression_algorithms:
                print(f"\n=== Testing compression={algo} ===\n")
                topic = f"nuclear-comp-{algo}"
                kcm.create_topic(topic=topic, partitions=1, replication=1)
                kcm.producer_perf_test(
                    topic=topic,
                    num_records=500,
                    record_size=256,
                    compression=algo,
                )
                size_kb = kcm.topic_partition_size(topic=topic, partition=0) / 1024
                print(f"Topic '{topic}' size: {size_kb:.2f} KB\n")
            kcm.stop()
            kcm.clean()

    elif args.cmd == 'stg4':
        kcm.init()
        if kcm.start():
            topic_partitions = [3, 6, 9]
            for partiotions in topic_partitions:
                print(f"\n=== Testing partioning={partiotions} ===\n")
                topic = f"nuclear-part-{partiotions}"
                kcm.create_topic(topic=topic, partitions=partiotions, replication=3)
                kcm.describe_topic(topic=topic)
                producer = kcm.create_producer()
                kcm.send_messages(
                    producer=producer,
                    topic=topic,
                    data_generator=generate_record,
                    num_messages=500,
                    flush_every=100,
                )
                counts = [kcm.topic_partition_messages(topic=topic, partition=i) for i in range(partiotions) ]
                print(f"Topic '{topic}' partition message counts: {counts}\n")
            kcm.stop()
            kcm.clean()


if __name__ == '__main__':
    main()
