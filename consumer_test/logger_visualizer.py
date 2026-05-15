from __future__ import annotations

import json
import os
from collections import deque
from datetime import datetime
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException
from rich.console import Console
from rich.live import Live
from rich.table import Table

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
KAFKA_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "psd-logger-visualizer")

# Uproszczony filtr: kwota >= 1000 PLN lub > 50% limitu karty
SUSPICIOUS_AMOUNT_MIN = float(os.getenv("SUSPICIOUS_AMOUNT_MIN", "1000"))
SUSPICIOUS_LIMIT_RATIO = float(os.getenv("SUSPICIOUS_LIMIT_RATIO", "0.5"))

VISIBLE_ROWS = 20


def is_suspicious(amount: float, card_limit: float) -> bool:
    return amount >= SUSPICIOUS_AMOUNT_MIN or amount > card_limit * SUSPICIOUS_LIMIT_RATIO


def parse_transaction(payload: bytes) -> dict[str, Any]:
    return json.loads(payload.decode("utf-8"))


def build_table(rows: deque[dict[str, Any]]) -> Table:
    table = Table(
        title=f"Kafka · {KAFKA_TOPIC}",
        show_header=True,
        header_style="bold cyan",
    )
    table.add_column("Time", style="dim")
    table.add_column("Card", justify="right")
    table.add_column("User", justify="right")
    table.add_column("Amount", justify="right")
    table.add_column("Limit", justify="right")
    table.add_column("Lat", justify="right")
    table.add_column("Lon", justify="right")
    table.add_column("Flag", justify="center")

    for tx in rows:
        amount = float(tx["amount"])
        limit = float(tx["card_limit"])
        suspicious = is_suspicious(amount, limit)
        ts = datetime.fromtimestamp(float(tx["timestamp"])).strftime("%H:%M:%S")

        row_style = "bold red" if suspicious else None
        flag = "[bold red]ALERT[/]" if suspicious else ""

        table.add_row(
            ts,
            str(tx["card_id"]),
            str(tx["user_id"]),
            f"{amount:,.2f}",
            f"{limit:,.2f}",
            f"{float(tx['lat']):.4f}",
            f"{float(tx['lon']):.4f}",
            flag,
            style=row_style,
        )

    return table


def create_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": KAFKA_GROUP,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        }
    )


def run() -> None:
    consumer = create_consumer()
    consumer.subscribe([KAFKA_TOPIC])
    console = Console()
    rows: deque[dict[str, Any]] = deque(maxlen=VISIBLE_ROWS)

    console.print(
        f"[green]Listening[/] on [bold]{KAFKA_TOPIC}[/] "
        f"({KAFKA_BOOTSTRAP}, group={KAFKA_GROUP})"
    )
    console.print(
        f"Suspicious if amount >= {SUSPICIOUS_AMOUNT_MIN:,.0f} "
        f"or amount > {SUSPICIOUS_LIMIT_RATIO:.0%} of card limit\n"
    )

    try:
        with Live(build_table(rows), console=console, refresh_per_second=8) as live:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                tx = parse_transaction(msg.value())
                rows.append(tx)
                live.update(build_table(rows))
    except KeyboardInterrupt:
        console.print("\n[yellow]Stopped.[/]")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
