from datetime import datetime

def map_to_event(row: dict):
    """
    Convert raw loan row → RecoverableEvent
    """

    loan_status = row.get("loan_status")

    # Map risk behavior
    if loan_status in ["Late", "Default", "Charged Off"]:
        event_type = "INVOICE_OVERDUE"
    else:
        event_type = "PAYMENT_RECEIVED"

    return {
        "event_id": str(row.get("id", "")),
        "event_type": event_type,
        "source": "LENDING_CLUB",
        "customer_id": str(row.get("member_id", "unknown")),
        "amount": float(row.get("loan_amnt", 0)),
        "currency": "USD",
        "status": loan_status,
        "timestamp": datetime.utcnow().isoformat(),
        "metadata": {
            "grade": row.get("grade"),
            "interest_rate": row.get("int_rate"),
            "term": row.get("term")
        }
    }