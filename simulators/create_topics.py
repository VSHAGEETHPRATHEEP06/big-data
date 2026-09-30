"""Create the Kafka topics used by the pipeline (safe to run many times).

fleet.telemetry.raw   6 partitions, key = vehicle_id. Six partitions let up to
                      six consumers read in parallel while every vehicle keeps
                      its event order inside one partition.
fleet.telemetry.dlq   1 partition. Dead-letter queue for events that fail
                      validation in the speed layer, kept for inspection/replay.
"""
from __future__ import annotations

import sys
import time

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from fleet_common.logs import get_logger
from fleet_common.settings import get_settings

log = get_logger("kafka-init", "ingestion")


def wait_for_broker(admin: AdminClient, timeout: float = 300.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            md = admin.list_topics(timeout=5)
            log.info("kafka_broker_ready", extra={"brokers": len(md.brokers)})
            return
        except KafkaException as exc:
            log.info("waiting_for_kafka", extra={"error": str(exc)})
            time.sleep(3)
    raise RuntimeError("Kafka broker not reachable")


def main() -> int:
    s = get_settings()
    admin = AdminClient({"bootstrap.servers": s.kafka_bootstrap})
    wait_for_broker(admin)
    retention_ms = str(s.topic_retention_hours * 3600 * 1000)
    wanted = [
        NewTopic(
            s.topic_telemetry,
            num_partitions=s.telemetry_partitions,
            replication_factor=1,
            config={
                "retention.ms": retention_ms,
                "cleanup.policy": "delete",
                "compression.type": "producer",
                "message.timestamp.type": "CreateTime",
                "min.insync.replicas": "1",
            },
        ),
        NewTopic(
            s.topic_dlq,
            num_partitions=1,
            replication_factor=1,
            config={"retention.ms": retention_ms, "cleanup.policy": "delete"},
        ),
    ]
    existing = admin.list_topics(timeout=10).topics
    missing = [t for t in wanted if t.topic not in existing]
    if missing:
        for topic, future in admin.create_topics(missing, operation_timeout=30).items():
            try:
                future.result()
                log.info("topic_created", extra={"topic": topic})
            except KafkaException as exc:
                if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                    raise
                log.info("topic_already_exists", extra={"topic": topic})

    metadata = admin.list_topics(timeout=10).topics
    for t in wanted:
        log.info(
            "topic_ready",
            extra={"topic": t.topic, "partitions": len(metadata[t.topic].partitions), "retention_ms": retention_ms},
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
