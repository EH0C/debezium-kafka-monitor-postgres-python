"""
Monitors the Debezium CDC topic for the orders table.
Prints an alert whenever an order with amount = 0 is inserted or updated.
"""

import json
import base64
import os
from kafka import KafkaConsumer
from decimal import Decimal

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "myserver.public.orders"
AMOUNT_SCALE = 2  # numeric(10,2) → divide unscaled int by 10^2


def extract_amount(after: dict) -> Decimal | None:
    """
    Debezium encodes numeric(10,2) as base64 big-endian bytes.
    e.g. "AA==" → b'\x00' → 0 → 0.00
         "AYaf" → 100495 → 1004.95
    """
    raw = after.get("amount")
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return Decimal(str(raw))
    # base64-encoded bytes from Debezium
    unscaled = int.from_bytes(base64.b64decode(raw), byteorder="big", signed=True)
    return Decimal(unscaled) / Decimal(10 ** AMOUNT_SCALE)


def notify(order: dict, op: str, amount: Decimal):
    op_label = {"c": "INSERT", "u": "UPDATE", "r": "SNAPSHOT"}.get(op, op)
    print(
        f"\n🚨 ALERT — amount = 0 detected!\n"
        f"  Operation : {op_label}\n"
        f"  Order ID  : {order.get('id')}\n"
        f"  Customer  : {order.get('customer_id')}\n"
        f"  Product   : {order.get('product')}\n"
        f"  Amount    : {amount}\n"
        f"  Status    : {order.get('status')}\n"
        f"  Created   : {order.get('created_at')}\n"
    )


def main():
    print(f"Connecting to Kafka at {BOOTSTRAP_SERVERS} ...")
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",       # catch up from the beginning
        enable_auto_commit=True,
        group_id="orders-monitor",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")) if b else None,
        consumer_timeout_ms=-1,             # block forever
    )
    print(f"Listening on topic '{TOPIC}' — will alert on amount = 0 ...\n")

    for message in consumer:
        envelope = message.value
        if not envelope:
            continue  # tombstone / delete marker

        payload = envelope.get("payload", envelope)  # unwrap Debezium envelope
        op = payload.get("op")          # c=create, u=update, d=delete, r=snapshot
        after = payload.get("after")

        if op == "d" or after is None:
            continue  # skip deletes

        amount = extract_amount(after)
        if amount is not None and amount == 0:
            notify(after, op, amount)
        else:
            print(f"  [ok] order id={after.get('id')} amount={after.get('amount')} op={op}")


if __name__ == "__main__":
    main()
