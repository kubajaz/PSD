from __future__ import annotations

import json
import os
import random
import time
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Optional

from confluent_kafka import Producer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")

NUM_CARDS = 10_000
NUM_USERS = 8_000
TX_PER_SECOND_MIN = 5
TX_PER_SECOND_MAX = 10

# Rough bounding box for Poland (home locations)
LAT_MIN, LAT_MAX = 49.0, 54.9
LON_MIN, LON_MAX = 14.1, 24.2


class AnomalyType(Enum):
    AMOUNT_SPIKE = "amount_spike"
    LOCATION_JUMP = "location_jump"
    HIGH_FREQUENCY = "high_frequency"


@dataclass
class Transaction:
    card_id: int
    user_id: int
    lat: float
    lon: float
    amount: float
    card_limit: float
    timestamp: float

    def to_json(self) -> str:
        return json.dumps(asdict(self))


@dataclass
class Card:
    card_id: int
    user_id: int
    card_limit: float
    home_lat: float
    home_lon: float
    avg_amount: float


@dataclass
class CardState:
    last_lat: float
    last_lon: float
    last_timestamp: float


def create_cards(num_cards: int, num_users: int) -> list[Card]:
    cards: list[Card] = []
    for card_id in range(num_cards):
        user_id = random.randint(0, num_users - 1)
        home_lat = random.uniform(LAT_MIN, LAT_MAX)
        home_lon = random.uniform(LON_MIN, LON_MAX)
        cards.append(
            Card(
                card_id=card_id,
                user_id=user_id,
                card_limit=round(random.uniform(2_000, 15_000), 2),
                home_lat=round(home_lat, 6),
                home_lon=round(home_lon, 6),
                avg_amount=round(random.uniform(20, 250), 2),
            )
        )
    return cards


def generate_anomaly(chance: float = 0.05) -> Optional[AnomalyType]:
    if random.random() >= chance:
        return None
    return random.choice(list(AnomalyType))


def _jitter_near_home(card: Card, radius_deg: float = 0.08) -> tuple[float, float]:
    lat = card.home_lat + random.uniform(-radius_deg, radius_deg)
    lon = card.home_lon + random.uniform(-radius_deg, radius_deg)
    return round(lat, 6), round(lon, 6)


def _normal_amount(card: Card) -> float:
    low = max(1.0, card.avg_amount * 0.4)
    high = min(card.card_limit, card.avg_amount * 1.6)
    return round(random.uniform(low, high), 2)


def _build_normal_transaction(
    card: Card,
    states: dict[int, CardState],
    timestamp: Optional[float] = None,
) -> Transaction:
    ts = timestamp if timestamp is not None else time.time()
    lat, lon = _jitter_near_home(card)
    amount = _normal_amount(card)
    states[card.card_id] = CardState(last_lat=lat, last_lon=lon, last_timestamp=ts)
    return Transaction(
        card_id=card.card_id,
        user_id=card.user_id,
        lat=lat,
        lon=lon,
        amount=amount,
        card_limit=card.card_limit,
        timestamp=ts,
    )


def _apply_amount_spike(card: Card, base: Transaction) -> Transaction:
    return Transaction(
        card_id=base.card_id,
        user_id=base.user_id,
        lat=base.lat,
        lon=base.lon,
        amount=round(card.avg_amount * 10, 2),
        card_limit=base.card_limit,
        timestamp=base.timestamp,
    )


def _apply_location_jump(
    card: Card,
    states: dict[int, CardState],
    base: Transaction,
) -> Transaction:
    state = states.get(card.card_id)
    if state is None:
        ref_lat, ref_lon = card.home_lat, card.home_lon
        ref_ts = base.timestamp - 30
    else:
        ref_lat, ref_lon = state.last_lat, state.last_lon
        ref_ts = state.last_timestamp

    # ~500–2000 km away within 1–3 minutes of the previous point
    jump_lat = ref_lat + random.choice([-1, 1]) * random.uniform(5.0, 18.0)
    jump_lon = ref_lon + random.choice([-1, 1]) * random.uniform(5.0, 18.0)
    ts = ref_ts + random.uniform(30, 180)

    tx = Transaction(
        card_id=card.card_id,
        user_id=card.user_id,
        lat=round(jump_lat, 6),
        lon=round(jump_lon, 6),
        amount=base.amount,
        card_limit=card.card_limit,
        timestamp=ts,
    )
    states[card.card_id] = CardState(
        last_lat=tx.lat, last_lon=tx.lon, last_timestamp=tx.timestamp
    )
    return tx


def _build_high_frequency_burst(
    card: Card,
    states: dict[int, CardState],
) -> list[Transaction]:
    burst_size = random.randint(6, 12)
    base_ts = time.time()
    transactions: list[Transaction] = []
    for i in range(burst_size):
        ts = base_ts + i * random.uniform(0.05, 0.25)
        tx = _build_normal_transaction(card, states, timestamp=ts)
        transactions.append(tx)
    return transactions


def generate_transactions(
    card: Card,
    states: dict[int, CardState],
    anomaly: Optional[AnomalyType],
) -> list[Transaction]:
    if anomaly is AnomalyType.HIGH_FREQUENCY:
        return _build_high_frequency_burst(card, states)

    base = _build_normal_transaction(card, states)

    if anomaly is AnomalyType.AMOUNT_SPIKE:
        return [_apply_amount_spike(card, base)]
    if anomaly is AnomalyType.LOCATION_JUMP:
        return [_apply_location_jump(card, states, base)]
    return [base]


def create_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "client.id": "psd-transaction-simulator",
        }
    )


def publish_transactions(producer: Producer, transactions: list[Transaction]) -> None:
    for tx in transactions:
        producer.produce(
            KAFKA_TOPIC,
            key=str(tx.card_id).encode("utf-8"),
            value=tx.to_json().encode("utf-8"),
        )
    producer.poll(0)


def run_loop(cards: list[Card]) -> None:
    producer = create_producer()
    states: dict[int, CardState] = {}
    print(
        f"Simulator started: {len(cards)} cards, topic={KAFKA_TOPIC}, "
        f"broker={KAFKA_BOOTSTRAP}, rate={TX_PER_SECOND_MIN}-{TX_PER_SECOND_MAX} tx/s"
    )

    try:
        while True:
            batch_start = time.time()
            batch_size = random.randint(TX_PER_SECOND_MIN, TX_PER_SECOND_MAX)
            batch: list[Transaction] = []

            for _ in range(batch_size):
                card = random.choice(cards)
                anomaly = generate_anomaly()
                batch.extend(generate_transactions(card, states, anomaly))

            publish_transactions(producer, batch)
            producer.flush()

            elapsed = time.time() - batch_start
            time.sleep(max(0.0, 1.0 - elapsed))
    except KeyboardInterrupt:
        print("\nStopping simulator…")
    finally:
        producer.flush()


def main() -> None:
    random.seed()
    cards = create_cards(NUM_CARDS, NUM_USERS)
    run_loop(cards)


if __name__ == "__main__":
    main()
