from datetime import datetime, timedelta

# In reality → query DB or cache
FAKE_DB = {}

async def build_features(event: dict):
    customer_id = event["customer_id"]

    history = FAKE_DB.get(customer_id, [])

    history.append(event)
    FAKE_DB[customer_id] = history

    now = datetime.utcnow()

    payments = [
        e for e in history
        if e["event_type"] == "PAYMENT_RECEIVED"
    ]

    invoices = [
        e for e in history
        if e["event_type"] == "INVOICE_CREATED"
    ]

    # Feature calculations
    payment_count_30d = len([
        p for p in payments
        if datetime.fromisoformat(p["event_timestamp"]) > now - timedelta(days=30)
    ])

    avg_payment = (
        sum(p.get("amount", 0) for p in payments) / len(payments)
        if payments else 0
    )

    last_payment_days = (
        (now - datetime.fromisoformat(payments[-1]["event_timestamp"])).days
        if payments else None
    )

    delinquency_ratio = (
        len(invoices) - len(payments)
    ) / len(invoices) if invoices else 0

    return {
        "customer_id": customer_id,
        "payment_frequency_30d": payment_count_30d,
        "avg_payment_amount": avg_payment,
        "days_since_last_payment": last_payment_days,
        "delinquency_ratio": delinquency_ratio
    }