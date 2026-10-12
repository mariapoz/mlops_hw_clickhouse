"""
Загрузка транзакций из CSV-файла в топик Kafka.

Каждая строка CSV превращается в одно JSON-сообщение (формат JSONEachRow),
например:
    {"transaction_time": "2019-12-27 15:21", "merch": "fraud_Cormier LLC",
     "cat_id": "health_fitness", "amount": "148.04", "us_state": "OR", ...}

Значения отправляются строками "как в CSV": типы приводит ClickHouse
при чтении из топика (см. sql/01_ddl.sql). Пустые значения -> "".

Запуск в docker (см. README):
    docker compose run --rm producer
Запуск с хоста:
    python producer/load_to_kafka.py --csv data/train.csv --bootstrap localhost:9095
"""
import argparse
import json
import logging
import os
import time

import pandas as pd
from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("producer")


def parse_args():
    parser = argparse.ArgumentParser(description="Load CSV rows into a Kafka topic")
    parser.add_argument("--csv", default=os.getenv("CSV_PATH", "data/train.csv"))
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9095"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "transactions"))
    parser.add_argument("--chunksize", type=int, default=50_000, help="Сколько строк CSV читать за раз")
    parser.add_argument("--limit", type=int, default=None, help="Отправить только первые N строк")
    return parser.parse_args()


def wait_for_kafka(bootstrap: str, topic: str, retries: int = 30, delay: float = 2.0):
    """Ждём, пока брокер поднимется и топик будет создан."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    for attempt in range(1, retries + 1):
        try:
            topics = admin.list_topics(timeout=5).topics
            if topic in topics:
                logger.info("Kafka is ready, topic '%s' exists", topic)
                return
            logger.info("Topic '%s' not found yet (attempt %d/%d)", topic, attempt, retries)
        except KafkaException as e:
            logger.info("Kafka not ready (attempt %d/%d): %s", attempt, retries, e)
        time.sleep(delay)
    raise RuntimeError(f"Kafka at {bootstrap} or topic '{topic}' is not available")


class DeliveryStats:
    def __init__(self):
        self.ok = 0
        self.failed = 0

    def callback(self, err, msg):
        if err is not None:
            self.failed += 1
            if self.failed <= 10:
                logger.error("Delivery failed: %s", err)
        else:
            self.ok += 1


def produce(producer: Producer, topic: str, value: str, stats: DeliveryStats):
    while True:
        try:
            producer.produce(topic, value=value.encode("utf-8"), on_delivery=stats.callback)
            return
        except BufferError:
            # Локальная очередь продюсера заполнена: ждём, пока часть сообщений уйдёт
            producer.poll(0.5)


def main():
    args = parse_args()
    if not os.path.exists(args.csv):
        raise FileNotFoundError(f"CSV file not found: {args.csv}")

    wait_for_kafka(args.bootstrap, args.topic)

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "linger.ms": 50,                 # копим сообщения в батчи
        "batch.num.messages": 10_000,
        "compression.type": "lz4",
        "queue.buffering.max.messages": 500_000,
    })
    stats = DeliveryStats()

    sent = 0
    started = time.time()
    # dtype=str + keep_default_na=False: значения уходят ровно как записаны в CSV
    reader = pd.read_csv(args.csv, dtype=str, keep_default_na=False, chunksize=args.chunksize)
    for chunk in reader:
        if args.limit is not None:
            chunk = chunk.head(args.limit - sent)
        for record in chunk.to_dict(orient="records"):
            produce(producer, args.topic, json.dumps(record, ensure_ascii=False), stats)
            sent += 1
        producer.poll(0)
        logger.info("Sent %d rows (%.0f rows/s)", sent, sent / (time.time() - started))
        if args.limit is not None and sent >= args.limit:
            break

    remaining = producer.flush(60)
    logger.info(
        "Done: %d rows read, %d delivered, %d failed, %d not flushed, %.1f s",
        sent, stats.ok, stats.failed, remaining, time.time() - started,
    )
    if stats.failed or remaining:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
