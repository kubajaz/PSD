from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_ALERT_TOPIC", "alerts")
KAFKA_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "psd-alert-monitor")

ASCII_BANNER = r"""
 ███████╗██████╗  █████╗ ██╗   ██╗██████╗     █████╗ ██╗     ███████╗██████╗ ████████╗
 ██╔════╝██╔══██╗██╔══██╗██║   ██║██╔══██╗   ██╔══██╗██║     ██╔════╝██╔══██╗╚══██╔══╝
 █████╗  ██████╔╝███████║██║   ██║██║  ██║   ███████║██║     █████╗  ██████╔╝   ██║
 ██╔══╝  ██╔══██╗██╔══██║██║   ██║██║  ██║   ██╔══██║██║     ██╔══╝  ██╔══██╗   ██║
 ██║     ██║  ██║██║  ██║╚██████╔╝██████╔╝   ██║  ██║███████╗███████╗██║  ██║   ██║
 ╚═╝     ╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝ ╚═════╝    ╚═╝  ╚═╝╚══════╝╚══════╝╚═╝  ╚═╝   ╚═╝
"""

ANOMALY_LABELS = {
    "LARGE_AMOUNT": "Wysoka kwota",
    "IMPOSSIBLE_TRAVEL": "Niemożliwa podróż",
    "HIGH_FREQUENCY": "Wysoka częstotliwość",
}


def play_alert_sound() -> None:
    console = Console()
    console.bell()
    for cmd in (
        ["paplay", "/usr/share/sounds/freedesktop/stereo/alarm-clock-elapsed.oga"],
        ["canberra-gtk-play", "--id", "dialog-warning"],
        ["printf", "\a"],
    ):
        try:
            if cmd[0] == "printf":
                subprocess.run(cmd, check=False)
            else:
                subprocess.run(
                    cmd,
                    check=False,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            return
        except FileNotFoundError:
            continue


def format_timestamp(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def build_reason(alert: dict[str, Any]) -> str:
    alert_type = alert.get("alert_type", "UNKNOWN")
    details: dict[str, Any] = alert.get("details") or {}

    if alert_type == "LARGE_AMOUNT":
        amount = details.get("amount", "?")
        threshold = details.get("threshold", "?")
        avg = details.get("rolling_avg", "?")
        return f"Kwota {amount} PLN > 3× średnia ({avg} PLN), próg: {threshold} PLN"

    if alert_type == "IMPOSSIBLE_TRAVEL":
        speed = details.get("speed_kmh", "?")
        distance = details.get("distance_km", "?")
        return f"Prędkość: {speed} km/h, dystans: {distance} km"

    if alert_type == "HIGH_FREQUENCY":
        sec = details.get("seconds_since_last", "?")
        return f"Kolejna transakcja po {sec} s (< {details.get('min_interval_sec', 10)} s)"

    return json.dumps(details, ensure_ascii=False) if details else "Wykryto nietypową aktywność"


def display_alert(alert: dict[str, Any]) -> None:
    console = Console()
    card_id = alert.get("card_id", "?")
    alert_type = alert.get("alert_type", "UNKNOWN")
    ts = alert.get("transaction_timestamp", 0)
    type_label = ANOMALY_LABELS.get(alert_type, alert_type)
    reason = build_reason(alert)

    body = Text()
    body.append(f"Karta (card_id):     ", style="bold white")
    body.append(f"{card_id}\n", style="bold yellow")
    body.append(f"Typ anomalii:        ", style="bold white")
    body.append(f"{type_label} ({alert_type})\n", style="bold red")
    body.append(f"Czas zdarzenia:      ", style="bold white")
    body.append(f"{format_timestamp(float(ts))}\n", style="cyan")
    body.append(f"Uzasadnienie:        ", style="bold white")
    body.append(f"{reason}\n", style="bold bright_red")

    panel = Panel(
        body,
        title="[bold white on red] ⚠  ALERT FRAUDOWY  ⚠ [/]",
        subtitle=f"[red]user_id={alert.get('user_id', '?')}[/]",
        border_style="bold red",
        box=box.DOUBLE,
        padding=(1, 2),
    )

    console.print()
    console.print(Text(ASCII_BANNER, style="bold red"))
    console.print(panel)
    console.print()


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

    console.print(
        f"[green]Monitor alertów[/] — temat [bold]{KAFKA_TOPIC}[/] "
        f"({KAFKA_BOOTSTRAP})"
    )
    console.print("[dim]Czekam na alerty… (Ctrl+C aby zakończyć)[/]\n")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            alert = json.loads(msg.value().decode("utf-8"))
            play_alert_sound()
            display_alert(alert)
    except KeyboardInterrupt:
        console.print("\n[yellow]Monitor zatrzymany.[/]")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
