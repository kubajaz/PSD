
from __future__ import annotations

import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from pyflink.common import Configuration, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.common.time import Duration
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import KeyedProcessFunction, MapFunction, RuntimeContext
from pyflink.datastream.state import ValueState, ValueStateDescriptor

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "psd")
MONGO_ALERTS_COLLECTION = os.getenv("MONGO_ALERTS_COLLECTION", "fraud_alerts")
SOURCE_TOPIC = os.getenv("KAFKA_SOURCE_TOPIC", "transactions")
SINK_TOPIC = os.getenv("KAFKA_SINK_TOPIC", "alerts")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "psd-fraud-detector")

AMOUNT_MULTIPLIER = float(os.getenv("FRAUD_AMOUNT_MULTIPLIER", "3"))
MAX_SPEED_KMH = float(os.getenv("FRAUD_MAX_SPEED_KMH", "500"))
MIN_TX_INTERVAL_SEC = float(os.getenv("FRAUD_MIN_TX_INTERVAL_SEC", "10"))
WATERMARK_DELAY_SEC = int(os.getenv("FRAUD_WATERMARK_DELAY_SEC", "5"))

JARS_DIR = Path(__file__).resolve().parent / "jars"

TRANSACTION_ROW_TYPE = Types.ROW_NAMED(
    ["card_id", "user_id", "lat", "lon", "amount", "card_limit", "timestamp"],
    [
        Types.INT(),
        Types.INT(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.DOUBLE(),
    ],
)

ALERT_TYPES = (
    "LARGE_AMOUNT",
    "IMPOSSIBLE_TRAVEL",
    "HIGH_FREQUENCY",
)


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_km = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    return 2 * radius_km * math.asin(min(1.0, math.sqrt(a)))


class TransactionTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value: Any, record_timestamp: int) -> int:
        # timestamp w JSON jest w sekundach (float)
        return int(value[6] * 1000)


class FraudDetector(KeyedProcessFunction):
    """KeyedProcessFunction — stan per card_id."""

    def open(self, runtime_context: RuntimeContext) -> None:
        self._last_lat: ValueState = runtime_context.get_state(
            ValueStateDescriptor("last_latitude", Types.DOUBLE())
        )
        self._last_lon: ValueState = runtime_context.get_state(
            ValueStateDescriptor("last_longitude", Types.DOUBLE())
        )
        self._last_ts: ValueState = runtime_context.get_state(
            ValueStateDescriptor("last_timestamp", Types.DOUBLE())
        )
        self._rolling_avg: ValueState = runtime_context.get_state(
            ValueStateDescriptor("rolling_avg_amount", Types.DOUBLE())
        )

    def process_element(
        self,
        value: Any,
        ctx: "KeyedProcessFunction.Context",
    ) -> Iterable[str]:
        card_id = int(value[0])
        user_id = int(value[1])
        lat = float(value[2])
        lon = float(value[3])
        amount = float(value[4])
        tx_timestamp = float(value[6])

        last_lat = self._last_lat.value()
        last_lon = self._last_lon.value()
        last_ts = self._last_ts.value()
        rolling_avg = self._rolling_avg.value()

        alerts: list[dict[str, Any]] = []

        if rolling_avg is not None and amount > rolling_avg * AMOUNT_MULTIPLIER:
            alerts.append(
                self._build_alert(
                    card_id,
                    user_id,
                    "LARGE_AMOUNT",
                    tx_timestamp,
                    amount=amount,
                    rolling_avg=round(rolling_avg, 2),
                    threshold=round(rolling_avg * AMOUNT_MULTIPLIER, 2),
                )
            )

        if (
            last_lat is not None
            and last_lon is not None
            and last_ts is not None
        ):
            delta_sec = tx_timestamp - last_ts
            if 0 < delta_sec < MIN_TX_INTERVAL_SEC:
                alerts.append(
                    self._build_alert(
                        card_id,
                        user_id,
                        "HIGH_FREQUENCY",
                        tx_timestamp,
                        seconds_since_last=round(delta_sec, 3),
                        min_interval_sec=MIN_TX_INTERVAL_SEC,
                    )
                )

            if delta_sec > 0:
                distance_km = haversine_km(last_lat, last_lon, lat, lon)
                speed_kmh = distance_km / (delta_sec / 3600.0)
                if speed_kmh > MAX_SPEED_KMH:
                    alerts.append(
                        self._build_alert(
                            card_id,
                            user_id,
                            "IMPOSSIBLE_TRAVEL",
                            tx_timestamp,
                            distance_km=round(distance_km, 2),
                            speed_kmh=round(speed_kmh, 2),
                            max_speed_kmh=MAX_SPEED_KMH,
                            from_lat=last_lat,
                            from_lon=last_lon,
                            to_lat=lat,
                            to_lon=lon,
                        )
                    )

        if rolling_avg is None:
            self._rolling_avg.update(amount)
        else:
            self._rolling_avg.update(rolling_avg + (amount - rolling_avg) * 0.1)

        self._last_lat.update(lat)
        self._last_lon.update(lon)
        self._last_ts.update(tx_timestamp)

        for alert in alerts:
            yield json.dumps(alert)

    @staticmethod
    def _build_alert(
        card_id: int,
        user_id: int,
        alert_type: str,
        transaction_timestamp: float,
        **details: Any,
    ) -> dict[str, Any]:
        return {
            "card_id": card_id,
            "user_id": user_id,
            "alert_type": alert_type,
            "transaction_timestamp": transaction_timestamp,
            "details": details,
        }


class MongoAlertPersist(MapFunction):
    """Zapis alertu do MongoDB (fraud_alerts), przekazuje dalej do Kafki."""

    def open(self, runtime_context: RuntimeContext) -> None:
        from pymongo import MongoClient

        self._client = MongoClient(MONGO_URI)
        self._collection = self._client[MONGO_DB][MONGO_ALERTS_COLLECTION]

    def map(self, value: str) -> str:
        alert = json.loads(value)
        alert["ingested_at"] = datetime.now(timezone.utc).isoformat()
        self._collection.insert_one(alert)
        return value

    def close(self) -> None:
        if hasattr(self, "_client"):
            self._client.close()


def _add_connector_jars(env: StreamExecutionEnvironment) -> None:
    jar_files = sorted(JARS_DIR.glob("*.jar"))
    if not jar_files:
        raise FileNotFoundError(
            f"Brak plików JAR w {JARS_DIR}. Zobacz instrukcję w nagłówku fraud_detector.py."
        )
    for jar in jar_files:
        env.add_jars(f"file://{jar.resolve()}")


def _build_kafka_source() -> KafkaSource:
    json_schema = (
        JsonRowDeserializationSchema.builder()
        .type_info(TRANSACTION_ROW_TYPE)
        .build()
    )
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(SOURCE_TOPIC)
        .set_group_id(CONSUMER_GROUP)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(json_schema)
        .build()
    )


def _build_kafka_sink() -> KafkaSink:
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(SINK_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )


def build_pipeline(env: StreamExecutionEnvironment) -> None:
    source = _build_kafka_source()
    kafka_sink = _build_kafka_sink()

    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(WATERMARK_DELAY_SEC)
        )
        .with_timestamp_assigner(TransactionTimestampAssigner())
        .with_idleness(Duration.of_seconds(30))
    )

    alerts = (
        env.from_source(source, watermark_strategy, "transactions")
        .key_by(lambda row: row[0], key_type=Types.INT())
        .process(FraudDetector(), output_type=Types.STRING())
        .name("fraud-detection")
    )

    (
        alerts.map(MongoAlertPersist(), output_type=Types.STRING())
        .name("alerts-mongo-persist")
        .sink_to(kafka_sink)
        .name("alerts-kafka-sink")
    )


def main() -> None:
    config = Configuration()
    config.set_string("execution.checkpointing.interval", "60000")

    env = StreamExecutionEnvironment.get_execution_environment(configuration=config)
    env.set_parallelism(int(os.getenv("FLINK_PARALLELISM", "1")))
    _add_connector_jars(env)

    build_pipeline(env)
    env.execute("PSD Fraud Detector")


if __name__ == "__main__":
    main()
